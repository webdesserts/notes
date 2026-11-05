//! Native-move coalescing buffer (P4f-1 — files only).
//!
//! macOS FSEvents (and the watcher built on it) give no rename linkage: a native
//! file move/rename surfaces as a `Deleted(old)` plus a `Modified(new)`, unlinked
//! and in EITHER order (see `watcher.rs`). Dispatched naively that is a tombstone
//! plus a fresh-UUID create — the document's lineage (its UUID + CRDT history) is
//! lost, and a peer re-downloads the full content under a new identity.
//!
//! This buffer re-stitches the pair. It holds not-yet-tracked creates and deletes
//! for a short window (`MOVE_WINDOW`), keyed by the document's portable
//! [`content_hash`](vault_sync::content_hash). When the second half of a pair
//! arrives within the window — in either order — the two collapse into ONE
//! same-UUID move (`Index::move_node`), which re-parents the existing node and
//! re-transfers zero content (INV-1). On window expiry with no partner, a buffered
//! event commits to its standalone meaning: a lone delete is a real tombstone, a
//! lone create is a real new document (never a silent drop).
//!
//! ## Determinism boundary (why this is safe)
//!
//! Move DETECTION is a local, wall-clock heuristic over ONE machine's event
//! stream — it never enters the convergence/merge math. Whatever crosses the wire
//! is either a `tree.mov` or a (tombstone + new node), both of which the
//! deterministic merge layer resolves to one converged state with content always
//! preserved. Two replicas can detect the same user action via different local
//! heuristics (one coalesces, the other window-misses and sees create+delete) and
//! still converge. So all timing/ordering logic lives here, in the daemon, and no
//! wall-clock value ever reaches a synced op's payload.
//!
//! ## Scope (P4f-1)
//!
//! Files only, in-memory only. No persistence/crash-recovery (P4f-2) and no
//! folder-move recognizer (P4f-3). The buffer is intentionally pure: it owns no
//! vault handle and performs no I/O. The caller (the daemon) computes content
//! hashes against the vault, feeds events in, and executes the [`MoveDecision`]s
//! and [`Expired`] records the buffer returns. That keeps the timing state machine
//! unit-testable in isolation.

use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use p2p_core::time_scale::scaled;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// How long a not-yet-tracked create/delete waits for its partner before it
/// commits standalone.
///
/// Must exceed the watcher's 200ms debounce period, or a move's two halves can't
/// both clear the debouncer and land in one window; 500ms gives ~2 debounce
/// periods of slack. The upper bound is the added propagation latency on a REAL
/// delete/create (it can't commit until proven not-a-move) — imperceptible for
/// notes and within the eventually-consistent envelope. Wrapped in
/// [`scaled`] so the time-dependent tests shrink it consistently; at the
/// production scale of 1.0 it is exactly 500ms.
const MOVE_WINDOW: Duration = Duration::from_millis(500);

/// A delete event awaiting a possible partner create. The node is NOT yet
/// tombstoned — the buffer holds the event so a later [`MoveDecision::Move`] can
/// re-parent the still-live node via `move_node`.
///
/// Carries the document's `uuid` so a crash-recovery journal ([`JournaledMove`])
/// can re-stitch the move's lineage on the next boot — the delete half is the
/// move's source, so its UUID is the identity the re-stitch re-attaches at the
/// new path (P4f-2).
#[derive(Debug, Clone)]
struct PendingDelete {
    old_path: String,
    uuid: Uuid,
    deadline: Instant,
}

/// A create event (a `Modified` at a not-yet-tracked path) awaiting a possible
/// partner delete. No node has been minted for it — the buffer holds the event so
/// a later [`MoveDecision::Move`] re-parents the deleted node onto this path
/// instead of minting a fresh UUID. The content is on disk (re-hashable), so we
/// hold no bytes.
#[derive(Debug, Clone)]
struct PendingCreate {
    new_path: String,
    deadline: Instant,
}

/// What the daemon should do in response to feeding an event into the coalescer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MoveDecision {
    /// A create⇄delete pair matched: execute ONE same-UUID move from `old_path`
    /// to `new_path` (`Index::move_node`). Re-parents the existing node, preserves
    /// its UUID + content, re-transfers nothing.
    Move { old_path: String, new_path: String },
    /// No partner yet — the event was buffered. Nothing to do until its partner
    /// arrives or its window expires (caught by the sweep).
    Buffered,
}

/// A buffered event whose window expired with no partner — it must now commit to
/// its standalone meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Expired {
    /// A lone delete past its window — a real deletion. Dispatch to the standalone
    /// delete sink (`on_file_deleted` → tombstone + broadcast).
    StandaloneDelete { path: String },
    /// A lone create past its window — a real new document. Dispatch to the
    /// standalone create sink (`on_file_modified` → mint fresh UUID + broadcast).
    StandaloneCreate { path: String },
}

/// Path of the move-coalescer's crash-recovery journal, relative to the vault
/// root. Mirrors `persistence.rs`'s `.sync/` path consts; the daemon writes it
/// through the vault's `FileSystem` (NOT `std::fs`), and JSON matches the design
/// (`known_peers.json` already establishes JSON-in-`.sync/`).
pub const PENDING_MOVES_FILE: &str = ".sync/pending-moves.json";

/// Schema version of the on-disk journal (`.sync/pending-moves.json`).
///
/// The journal is **ephemeral, single-writer, same-version** — written and read by
/// the SAME daemon binary across a crash/restart, never synced and never read by a
/// peer or an older binary. So strict forward-compat is not required; the version
/// is a cheap guard so a future format change is detectable. On load, a `version`
/// mismatch means "discard the journal (treat as empty)" rather than mis-parse —
/// degrading to the crash-tail the design already accepts. (P4f-2 §1.1.)
pub const PENDING_MOVES_VERSION: u32 = 1;

/// Which pending map a [`JournaledMove`] mirrors. A categorical value, serialized
/// via serde rename rather than a bare string so the kind can never be a stray
/// free-text reference.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum PendingKind {
    #[serde(rename = "delete")]
    Delete,
    #[serde(rename = "create")]
    Create,
}

/// A serializable mirror of ONE buffered create/delete, persisted to the
/// crash-recovery journal so a move buffered in-memory survives a daemon crash
/// within its window (P4f-2). The journal is written by the daemon observing the
/// coalescer's [`MoveCoalescer::snapshot`] — the coalescer itself stays pure and
/// performs no I/O.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournaledMove {
    /// Which pending map this came from.
    pub kind: PendingKind,
    /// The doc's content hash, lowercase hex (32 bytes → 64-char hex) for
    /// JSON-friendliness — `[u8; 32]` has no clean compact JSON form, and hex is
    /// the crate's established convention for byte-array identifiers.
    pub content_hash: String,
    /// `old_path` for a delete, `new_path` for a create.
    pub path: String,
    /// The original doc UUID — the lineage 2b's boot re-stitch re-attaches.
    /// `Some` and REQUIRED on a delete (the move's source, the identity to
    /// re-attach). `None` on a create whose partner delete had not arrived when the
    /// snapshot was taken (so its UUID was unknown); the re-stitch keys off the
    /// delete record's UUID, so a `None` here is acceptable.
    ///
    /// Stored as the hyphenated `Uuid::to_string` form (not the hex `content_hash`
    /// convention above) — the recovery boundary decodes it back via
    /// `Uuid::parse_str`, so the round-trip is intact.
    pub uuid: Option<String>,
}

/// The on-disk journal file shape: a versioned wrapper around the pending set.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingMovesFile {
    pub version: u32,
    pub pending: Vec<JournaledMove>,
}

/// Lowercase-hex encode a 32-byte content hash for the journal's `content_hash`
/// field — the same per-byte `{:02x}` convention the crate uses elsewhere (e.g.
/// `PeerId`'s `Display`).
///
/// `pub` so the boot-recovery round-trip test and the integration harness can build
/// a faithful journal record using the journal's own encoder (its inverse is
/// `move_recovery::hex_to_bytes32`).
pub fn hex_lower(bytes: &[u8; 32]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Bounded, in-memory create⇄delete pairing buffer (P4f-1).
///
/// Keyed by content hash so the two halves of a move pair regardless of arrival
/// order. Each hash bucket is a FIFO queue so two identical-content files in one
/// window pair 1:1 without losing either (the pathological collision case is
/// hardened further in P4f-3; FIFO is the safe first cut).
pub(crate) struct MoveCoalescer {
    pending_deletes: HashMap<[u8; 32], VecDeque<PendingDelete>>,
    pending_creates: HashMap<[u8; 32], VecDeque<PendingCreate>>,
}

impl MoveCoalescer {
    pub(crate) fn new() -> Self {
        Self {
            pending_deletes: HashMap::new(),
            pending_creates: HashMap::new(),
        }
    }

    /// The sweep cadence — a fraction of the window so an expiry is caught
    /// promptly rather than up to a full window late. Wrapped in [`scaled`] to
    /// track the test time-scale alongside [`MOVE_WINDOW`].
    pub(crate) fn sweep_interval() -> Duration {
        scaled(MOVE_WINDOW / 2)
    }

    /// Whether a delete for `path` is currently buffered.
    ///
    /// The router consults this so a `Modified` at a path with a still-live node
    /// but a buffered delete (a re-create at a just-deleted path, OQ-D) is treated
    /// as a create-candidate rather than the edit fast-path — the node is only
    /// "live" because the buffer is deliberately holding its deletion.
    pub(crate) fn has_pending_delete(&self, path: &str) -> bool {
        self.pending_deletes
            .values()
            .any(|q| q.iter().any(|d| d.old_path == path))
    }

    /// Feed a delete event (the old half of a possible move).
    ///
    /// `hash` is the content hash of the doc at `path`, computed by the caller
    /// from its still-present `ContentDoc` BEFORE any tombstone. `uuid` is the
    /// doc's identity, captured in the same vault lock that hashed it — held so the
    /// crash-recovery journal can re-stitch the move's lineage on boot (P4f-2). If a
    /// buffered create already matches that hash, the pair fires now (the create-
    /// arrived-first case); otherwise the delete is buffered for one window.
    pub(crate) fn on_delete(&mut self, hash: [u8; 32], path: &str, uuid: Uuid) -> MoveDecision {
        if let Some(create) = self.pop_pending_create(&hash) {
            return MoveDecision::Move {
                old_path: path.to_string(),
                new_path: create.new_path,
            };
        }
        self.pending_deletes
            .entry(hash)
            .or_default()
            .push_back(PendingDelete {
                old_path: path.to_string(),
                uuid,
                deadline: Instant::now() + scaled(MOVE_WINDOW),
            });
        MoveDecision::Buffered
    }

    /// Feed a create event (the new half of a possible move).
    ///
    /// `hash` is the content hash of the new on-disk `.md`, computed by the caller
    /// in the same hash domain as the delete side. If a buffered delete already
    /// matches, the pair fires now (the common delete-arrived-first case);
    /// otherwise the create is buffered for one window.
    pub(crate) fn on_create(&mut self, hash: [u8; 32], path: &str) -> MoveDecision {
        if let Some(delete) = self.pop_pending_delete(&hash) {
            return MoveDecision::Move {
                old_path: delete.old_path,
                new_path: path.to_string(),
            };
        }
        self.pending_creates
            .entry(hash)
            .or_default()
            .push_back(PendingCreate {
                new_path: path.to_string(),
                deadline: Instant::now() + scaled(MOVE_WINDOW),
            });
        MoveDecision::Buffered
    }

    /// Drain every buffered event whose deadline has passed, returning each one's
    /// standalone meaning. Called from the daemon's sweep-timer arm.
    pub(crate) fn sweep(&mut self) -> Vec<Expired> {
        let now = Instant::now();
        let mut expired = Vec::new();

        retain_expired(&mut self.pending_deletes, now, |d| {
            expired.push(Expired::StandaloneDelete {
                path: d.old_path.clone(),
            });
        });
        retain_expired(&mut self.pending_creates, now, |c| {
            expired.push(Expired::StandaloneCreate {
                path: c.new_path.clone(),
            });
        });

        expired
    }

    /// Empty BOTH pending maps unconditionally, returning every buffered record's
    /// standalone meaning — the graceful-shutdown drain (P4f-2c). Unlike [`sweep`],
    /// this ignores deadlines: a clean quit force-expires the whole pending set so
    /// the buffer is empty at exit. Safe because pairing is eager — a completable
    /// create⇄delete pair never co-resides in the buffer (the second half to arrive
    /// fires the move immediately), so at any quiescent moment the maps hold only
    /// unpaired singletons. The drain therefore only ever finalizes lone records;
    /// there is nothing to wait for, so it is wait-free.
    pub(crate) fn drain_all(&mut self) -> Vec<Expired> {
        let mut drained = Vec::new();
        for (_, queue) in self.pending_deletes.drain() {
            for delete in queue {
                drained.push(Expired::StandaloneDelete {
                    path: delete.old_path,
                });
            }
        }
        for (_, queue) in self.pending_creates.drain() {
            for create in queue {
                drained.push(Expired::StandaloneCreate {
                    path: create.new_path,
                });
            }
        }
        drained
    }

    /// Serialize the current pending set into journal records (a pure read).
    ///
    /// The daemon calls this after every buffer mutation and writes the result to
    /// `.sync/pending-moves.json` (the daemon owns all I/O — the coalescer stays
    /// pure). Because every persist rewrites the WHOLE file from this snapshot,
    /// pruning a committed record is automatic: a record that has left the maps
    /// (paired into a move, expired, or drained) is simply absent here, so the next
    /// write omits it (§1.4). A delete record carries its UUID — the lineage the
    /// boot re-stitch re-attaches; a create carries `None` (the coalescer holds no
    /// UUID for an unpaired create).
    pub(crate) fn snapshot(&self) -> Vec<JournaledMove> {
        let mut records = Vec::new();
        for (hash, queue) in &self.pending_deletes {
            let content_hash = hex_lower(hash);
            for delete in queue {
                records.push(JournaledMove {
                    kind: PendingKind::Delete,
                    content_hash: content_hash.clone(),
                    path: delete.old_path.clone(),
                    uuid: Some(delete.uuid.to_string()),
                });
            }
        }
        for (hash, queue) in &self.pending_creates {
            let content_hash = hex_lower(hash);
            for create in queue {
                records.push(JournaledMove {
                    kind: PendingKind::Create,
                    content_hash: content_hash.clone(),
                    path: create.new_path.clone(),
                    uuid: None,
                });
            }
        }
        records
    }

    /// Pop the oldest buffered create for `hash`, removing the bucket if it empties.
    fn pop_pending_create(&mut self, hash: &[u8; 32]) -> Option<PendingCreate> {
        let queue = self.pending_creates.get_mut(hash)?;
        let create = queue.pop_front();
        if queue.is_empty() {
            self.pending_creates.remove(hash);
        }
        create
    }

    /// Pop the oldest buffered delete for `hash`, removing the bucket if it empties.
    fn pop_pending_delete(&mut self, hash: &[u8; 32]) -> Option<PendingDelete> {
        let queue = self.pending_deletes.get_mut(hash)?;
        let delete = queue.pop_front();
        if queue.is_empty() {
            self.pending_deletes.remove(hash);
        }
        delete
    }
}

/// Remove every record in `map` whose deadline has passed, invoking `on_expire`
/// for each, and drop any bucket that empties.
fn retain_expired<T, F>(map: &mut HashMap<[u8; 32], VecDeque<T>>, now: Instant, mut on_expire: F)
where
    T: HasDeadline,
    F: FnMut(&T),
{
    map.retain(|_, queue| {
        queue.retain(|record| {
            if record.deadline() <= now {
                on_expire(record);
                false
            } else {
                true
            }
        });
        !queue.is_empty()
    });
}

/// Shared deadline accessor so the sweep can treat pending creates and deletes
/// uniformly.
trait HasDeadline {
    fn deadline(&self) -> Instant;
}

impl HasDeadline for PendingDelete {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}

impl HasDeadline for PendingCreate {
    fn deadline(&self) -> Instant {
        self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_A: [u8; 32] = [0xAA; 32];
    const HASH_B: [u8; 32] = [0xBB; 32];

    /// A stable test UUID for the delete half of a move. The coalescer only
    /// carries it through to [`MoveCoalescer::snapshot`]; the pairing logic never
    /// inspects it, so any fixed value exercises the path.
    fn uuid_a() -> Uuid {
        Uuid::from_u128(0xA)
    }

    /// The common case: a delete buffers, then a content-matching create pairs
    /// with it into a single move (delete-arrived-first).
    #[test]
    fn delete_then_matching_create_emits_move() {
        let mut c = MoveCoalescer::new();
        assert_eq!(
            c.on_delete(HASH_A, "old.md", uuid_a()),
            MoveDecision::Buffered
        );
        assert_eq!(
            c.on_create(HASH_A, "new.md"),
            MoveDecision::Move {
                old_path: "old.md".to_string(),
                new_path: "new.md".to_string(),
            }
        );
        // Both halves consumed — nothing left to sweep.
        assert!(c.sweep().is_empty());
    }

    /// The symmetry the old design couldn't express: the create half can arrive
    /// first and still pair into the same move.
    #[test]
    fn create_then_matching_delete_emits_move() {
        let mut c = MoveCoalescer::new();
        assert_eq!(c.on_create(HASH_A, "new.md"), MoveDecision::Buffered);
        assert_eq!(
            c.on_delete(HASH_A, "old.md", uuid_a()),
            MoveDecision::Move {
                old_path: "old.md".to_string(),
                new_path: "new.md".to_string(),
            }
        );
        assert!(c.sweep().is_empty());
    }

    /// A delete with no content-matching create expires to a real tombstone.
    #[test]
    fn lone_delete_expires_to_standalone_delete() {
        let mut c = MoveCoalescer::new();
        c.on_delete(HASH_A, "gone.md", uuid_a());
        c.expire_all_for_test();
        assert_eq!(
            c.sweep(),
            vec![Expired::StandaloneDelete {
                path: "gone.md".to_string()
            }]
        );
    }

    /// A create with no content-matching delete expires to a real new document.
    #[test]
    fn lone_create_expires_to_standalone_create() {
        let mut c = MoveCoalescer::new();
        c.on_create(HASH_A, "fresh.md");
        c.expire_all_for_test();
        assert_eq!(
            c.sweep(),
            vec![Expired::StandaloneCreate {
                path: "fresh.md".to_string()
            }]
        );
    }

    /// A same-content create arriving AFTER the delete's window expired finds
    /// nothing pending — it is a NEW document, not an adoption of the old lineage.
    /// (The EC-8 ancient-coincidence non-adopt, at the buffer level.)
    #[test]
    fn matching_create_after_window_does_not_pair() {
        let mut c = MoveCoalescer::new();
        c.on_delete(HASH_A, "old.md", uuid_a());
        // The delete's window elapses and it commits standalone.
        c.expire_all_for_test();
        assert_eq!(
            c.sweep(),
            vec![Expired::StandaloneDelete {
                path: "old.md".to_string()
            }]
        );
        // A later same-content create has no pending delete to adopt onto.
        assert_eq!(c.on_create(HASH_A, "other.md"), MoveDecision::Buffered);
    }

    /// Non-matching content does not pair: a delete and a create with different
    /// hashes both sit buffered until their windows expire standalone.
    #[test]
    fn different_content_does_not_pair() {
        let mut c = MoveCoalescer::new();
        assert_eq!(
            c.on_delete(HASH_A, "a.md", uuid_a()),
            MoveDecision::Buffered
        );
        assert_eq!(c.on_create(HASH_B, "b.md"), MoveDecision::Buffered);
        c.expire_all_for_test();
        let mut expired = c.sweep();
        expired.sort_by_key(|e| format!("{e:?}"));
        assert_eq!(
            expired,
            vec![
                Expired::StandaloneCreate {
                    path: "b.md".to_string()
                },
                Expired::StandaloneDelete {
                    path: "a.md".to_string()
                },
            ]
        );
    }

    /// The graceful-shutdown drain force-expires BOTH buffered singletons to their
    /// standalone meanings and leaves the buffer empty — a clean quit resolves every
    /// pending record (P4f-2c).
    #[test]
    fn drain_all_empties_both_maps_to_standalone() {
        let mut c = MoveCoalescer::new();
        assert_eq!(
            c.on_delete(HASH_A, "old.md", uuid_a()),
            MoveDecision::Buffered
        );
        assert_eq!(c.on_create(HASH_B, "new.md"), MoveDecision::Buffered);

        let mut drained = c.drain_all();
        drained.sort_by_key(|e| format!("{e:?}"));
        assert_eq!(
            drained,
            vec![
                Expired::StandaloneCreate {
                    path: "new.md".to_string()
                },
                Expired::StandaloneDelete {
                    path: "old.md".to_string()
                },
            ]
        );
        // Both maps emptied — nothing remains for a follow-up snapshot or sweep.
        assert!(c.snapshot().is_empty());
        assert!(c.sweep().is_empty());
    }

    /// The drain is deadline-INDEPENDENT: a just-buffered record whose window has
    /// NOT elapsed still drains immediately. This is the wait-free property at the
    /// unit level — the drain never waits for a deadline (contrast `sweep`, which
    /// leaves the same fresh record buffered).
    #[test]
    fn drain_all_ignores_deadlines() {
        let mut c = MoveCoalescer::new();
        c.on_delete(HASH_A, "fresh.md", uuid_a());
        // Sanity: the window has not elapsed, so a sweep would NOT touch this record.
        // (No `expire_all_for_test` call — the deadline is genuinely in the future.)

        let drained = c.drain_all();
        assert_eq!(
            drained,
            vec![Expired::StandaloneDelete {
                path: "fresh.md".to_string()
            }]
        );
    }

    /// A clean quit with nothing buffered drains to an empty result — guards the
    /// common no-pending-records shutdown path.
    #[test]
    fn drain_all_on_empty_is_noop() {
        let mut c = MoveCoalescer::new();
        assert_eq!(c.drain_all(), vec![]);
    }

    /// `snapshot()` mirrors the live pending set into the serializable journal
    /// records, and those records survive a JSON round-trip unchanged — the
    /// property the crash-recovery journal rests on. The delete record MUST carry
    /// its UUID (the lineage 2b's boot re-stitch re-attaches); a create with no
    /// known partner carries `None`.
    #[test]
    fn snapshot_round_trips_pending_records() {
        let mut c = MoveCoalescer::new();
        c.on_delete(HASH_A, "old.md", uuid_a());
        c.on_create(HASH_B, "new.md");

        let snapshot = c.snapshot();
        assert_eq!(snapshot.len(), 2, "both pending halves should be journaled");

        let delete = snapshot
            .iter()
            .find(|m| m.kind == PendingKind::Delete)
            .expect("the buffered delete should be in the snapshot");
        assert_eq!(delete.path, "old.md");
        assert_eq!(delete.content_hash, hex_lower(&HASH_A));
        assert_eq!(
            delete.uuid.as_deref(),
            Some(uuid_a().to_string().as_str()),
            "a delete record carries the moved doc's UUID (the 2b lineage)"
        );

        let create = snapshot
            .iter()
            .find(|m| m.kind == PendingKind::Create)
            .expect("the buffered create should be in the snapshot");
        assert_eq!(create.path, "new.md");
        assert_eq!(create.content_hash, hex_lower(&HASH_B));
        assert_eq!(
            create.uuid, None,
            "a create with no paired delete has no known UUID"
        );

        // The on-disk shape is the `{version, pending}` wrapper — round-trip it.
        let file = PendingMovesFile {
            version: PENDING_MOVES_VERSION,
            pending: snapshot.clone(),
        };
        let json = serde_json::to_string(&file).expect("serialize");
        let restored: PendingMovesFile = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(restored.version, PENDING_MOVES_VERSION);
        assert_eq!(restored.pending, snapshot);
    }

    /// A record that has been committed (here: a delete paired into a move, so both
    /// halves are popped) is absent from the next snapshot. This is what makes
    /// pruning fall out of the full-rewrite-from-snapshot persist (§1.4): a
    /// committed record can never linger in the journal to be re-committed on boot.
    #[test]
    fn snapshot_excludes_committed_records() {
        let mut c = MoveCoalescer::new();
        c.on_delete(HASH_A, "old.md", uuid_a());
        assert!(
            !c.snapshot().is_empty(),
            "the buffered delete is journaled before it pairs"
        );

        // Pairing consumes both halves.
        assert!(matches!(
            c.on_create(HASH_A, "new.md"),
            MoveDecision::Move { .. }
        ));
        assert!(
            c.snapshot().is_empty(),
            "a committed (paired) record must not remain in the snapshot"
        );
    }

    /// `has_pending_delete` reports a buffered delete by path — the signal the
    /// router uses to route a re-create-at-a-just-deleted-path (OQ-D) to the
    /// create branch instead of the edit fast-path.
    #[test]
    fn has_pending_delete_tracks_buffered_deletes() {
        let mut c = MoveCoalescer::new();
        assert!(!c.has_pending_delete("p.md"));
        c.on_delete(HASH_A, "p.md", uuid_a());
        assert!(c.has_pending_delete("p.md"));
        // Pairing it consumes the pending delete.
        c.on_create(HASH_A, "p.md");
        assert!(!c.has_pending_delete("p.md"));
    }

    impl MoveCoalescer {
        /// Force every buffered record's deadline into the past so the next
        /// `sweep()` expires it — lets the unit tests exercise expiry without
        /// sleeping. The integration tests drive the real (scaled) window.
        fn expire_all_for_test(&mut self) {
            let past = Instant::now() - Duration::from_secs(3600);
            for queue in self.pending_deletes.values_mut() {
                for record in queue.iter_mut() {
                    record.deadline = past;
                }
            }
            for queue in self.pending_creates.values_mut() {
                for record in queue.iter_mut() {
                    record.deadline = past;
                }
            }
        }
    }
}
