//! Outbound message construction: the request that opens a handshake, the
//! exchange/response builders that compute what a peer is missing, and the
//! real-time push primitives.
//!
//! Carried from the legacy sync crate's `sync_engine/prepare.rs`, re-keyed on [`DocId`]
//! (UUID) instead of path. The headline carry is the send-side node-durability
//! coupling (S5/C3): when a response ships any brand-new document snapshot, it also
//! ships the FULL Index snapshot so the node lands with the doc — the send half of
//! the Flow-2 gate (INV-8).

use crate::events::SyncEvent;
use crate::fs::FileSystem;
use crate::vault::Vault;

use std::collections::{HashMap, HashSet};
use tracing::warn;

use super::{DocId, Result, SyncError, SyncMessage, SyncRequestData, SyncResponseData};

impl<F: FileSystem> Vault<F> {
    /// Prepare a sync request to open a handshake with a peer.
    ///
    /// Serializes a `SyncRequest` carrying our whole-vault catalog digest plus our Index
    /// version vector, and DELIBERATELY no per-document version vectors — the digest is
    /// the no-op discriminator (§6.2 / OQ-C1), so the common steady-state opener stays
    /// O(1) wire payload instead of O(document-count). The peer answers with `InSync`
    /// (digests matched — done) or `DigestMismatch` (revealing its VVs so we can drive
    /// the exchange).
    pub async fn prepare_request(&self) -> Result<Vec<u8>> {
        let msg = SyncMessage::SyncRequest {
            catalog_digest: self.catalog_digest(),
            index_version: self.index_version(),
            document_versions: HashMap::new(),
        };

        let bytes =
            bincode::serialize(&msg).map_err(|e| SyncError::Serialization(e.to_string()))?;

        self.emit(SyncEvent::MessageSent {
            message_type: "SyncRequest".into(),
            size: bytes.len(),
            timestamp: self.now_ms(),
        });

        Ok(bytes)
    }

    /// Prepare a real-time push for a single document's current state.
    ///
    /// Ships a full snapshot keyed by UUID. Carries NO Index node (S5): a lone push
    /// for a brand-new document is hard-skipped by the receiver's Flow-2 gate and
    /// recovered by the next full sync / boot reconcile. Returns `None` if the UUID
    /// resolves to no current path (nothing to push).
    pub async fn prepare_doc_update(&self, uuid: DocId) -> Result<Option<Vec<u8>>> {
        let Some(path) = self.path_for_doc(&uuid) else {
            return Ok(None);
        };

        let doc = self.get_document(&path).await?;
        let snapshot = doc.export_snapshot()?;

        let msg = SyncMessage::DocUpdate {
            uuid,
            data: snapshot,
        };
        let bytes =
            bincode::serialize(&msg).map_err(|e| SyncError::Serialization(e.to_string()))?;

        self.emit(SyncEvent::MessageSent {
            message_type: "DocUpdate".into(),
            size: bytes.len(),
            timestamp: self.now_ms(),
        });

        Ok(Some(bytes))
    }

    /// Prepare a document-deletion push to broadcast.
    pub fn prepare_doc_deleted(&self, uuid: DocId) -> Result<Vec<u8>> {
        let msg = SyncMessage::DocDeleted { uuid };
        let bytes =
            bincode::serialize(&msg).map_err(|e| SyncError::Serialization(e.to_string()))?;

        self.emit(SyncEvent::FileOp {
            operation: "delete".into(),
            path: uuid.to_string(),
            new_path: None,
            timestamp: self.now_ms(),
        });

        Ok(bytes)
    }

    /// Prepare a move push to broadcast.
    ///
    /// A move is a pure-structural Index op, so the wire carries the Index move-op
    /// (the whole Index delta), NOT a path-pair message — the receiver applies the
    /// `tree.mov` from the delta and its caches re-derive the new path. This ships
    /// the full Index snapshot so the move lands regardless of the receiver's Index
    /// version (the same node-durability guarantee as a new-doc snapshot). The
    /// document's content `.loro` is untouched (INV-1).
    pub fn prepare_doc_moved(&self, uuid: DocId, new_path: &str) -> Result<Vec<u8>> {
        let index_snapshot = self
            .index()
            .export_snapshot()
            .map_err(|e| SyncError::Serialization(e.to_string()))?;

        let msg = SyncMessage::SyncResponse {
            index_updates: Some(index_snapshot),
            document_updates: HashMap::new(),
        };
        let bytes =
            bincode::serialize(&msg).map_err(|e| SyncError::Serialization(e.to_string()))?;

        self.emit(SyncEvent::FileOp {
            operation: "rename".into(),
            path: uuid.to_string(),
            new_path: Some(new_path.to_string()),
            timestamp: self.now_ms(),
        });

        Ok(bytes)
    }

    /// The Index version vector as bytes.
    fn index_version(&self) -> Vec<u8> {
        self.index().state_vv().encode()
    }

    /// Prepare a `SyncExchange` in response to a peer's `SyncRequest`.
    ///
    /// Bundles our response (updates they need from us) with our request (our
    /// version vectors so they can send us updates they have).
    pub(super) async fn prepare_sync_exchange(
        &self,
        their_index_version: &[u8],
        their_versions: HashMap<DocId, Vec<u8>>,
    ) -> Result<SyncMessage> {
        let response = self
            .prepare_response_data(their_index_version, their_versions)
            .await?;
        let request = self.prepare_request_data().await?;
        Ok(SyncMessage::SyncExchange { response, request })
    }

    /// Prepare our request data (our version vectors), keyed by document UUID.
    ///
    /// Enumerates per-document VVs, so it is O(document-count) — used by the
    /// digest-MISS path (`DigestMismatch` reveals these so the opener can compute
    /// minimal deltas) and inside `prepare_sync_exchange`, NOT by the O(1) opener.
    pub(super) async fn prepare_request_data(&self) -> Result<SyncRequestData> {
        let index_version = self.index_version();
        let mut document_versions = HashMap::new();

        for path in self.list_files().await? {
            let Some(uuid) = self.uuid_for_path(&path) else {
                // A file on disk with no Index node (e.g. created but not yet
                // documented). Skipped here; boot reconcile / Flow-1 documents it.
                continue;
            };
            let doc = self.get_document(&path).await?;
            document_versions.insert(DocId(uuid), doc.version().encode());
        }

        Ok(SyncRequestData {
            index_version,
            document_versions,
        })
    }

    /// Prepare response data (updates the peer is missing).
    async fn prepare_response_data(
        &self,
        their_index_version: &[u8],
        their_versions: HashMap<DocId, Vec<u8>>,
    ) -> Result<SyncResponseData> {
        self.prepare_response_data_excluding(their_index_version, their_versions, &HashSet::new())
            .await
    }

    /// Prepare response data, excluding documents we just received.
    ///
    /// Used when answering a `SyncExchange`: we exclude documents we just imported
    /// so we don't echo them back. Loro's import creates a local change marker, so a
    /// naive version comparison would otherwise re-send updates for documents we
    /// just received.
    pub(super) async fn prepare_response_data_excluding(
        &self,
        their_index_version: &[u8],
        their_versions: HashMap<DocId, Vec<u8>>,
        exclude: &HashSet<DocId>,
    ) -> Result<SyncResponseData> {
        let mut document_updates = HashMap::new();

        // Tracks whether this response ships a brand-new document (a full snapshot
        // for a UUID the peer lacks). The Index node-create op for such a document
        // rides a resend-once VV-delta, so a peer whose Index VV already "covers" the
        // op without it having landed would get the doc but not the node. When any
        // new doc ships, we force a full Index snapshot below so the node is as
        // resend-durable as its content — the send-side guarantee the Flow-2 apply
        // gate in `apply_doc.rs` depends on (it hard-skips a new doc whose node is
        // absent).
        let mut sent_new_doc_snapshot = false;

        for path in self.list_files().await? {
            let Some(uuid) = self.uuid_for_path(&path) else {
                continue;
            };
            let doc_id = DocId(uuid);

            // Skip documents we just received (their import marker would otherwise
            // look like an update we owe the peer).
            if exclude.contains(&doc_id) {
                continue;
            }

            let doc = self.get_document(&path).await?;

            if let Some(their_version_bytes) = their_versions.get(&doc_id) {
                // They have it — send updates since their version, but ONLY when we
                // actually hold ops they lack. Gating on version-vector inclusion
                // (not on a byte-length check) is what keeps a pure MOVE zero-content:
                // `export_updates(their_version)` returns a non-empty (~22-byte)
                // FRAMED-EMPTY payload even when there is nothing new, so a byte-length
                // (`is_empty()`) guard would ship that no-op for every already-converged
                // document on every sync. `includes_vv` is the semantically correct
                // condition: if their version already covers ours, we owe them nothing —
                // and once it does NOT, the export is necessarily non-empty.
                if let Ok(their_version) = loro::VersionVector::decode(their_version_bytes)
                    && !their_version.includes_vv(&doc.version())
                {
                    document_updates.insert(doc_id, doc.export_updates(&their_version)?);
                }
            } else {
                // They lack it — send a full snapshot.
                document_updates.insert(doc_id, doc.export_snapshot()?);
                sent_new_doc_snapshot = true;
            }
        }

        let index_updates = self
            .export_index_updates(their_index_version, sent_new_doc_snapshot)
            .await;

        Ok(SyncResponseData {
            index_updates,
            document_updates,
        })
    }

    /// Export the Index updates a response should carry.
    ///
    /// When the response ships any new-document snapshot, send the FULL Index
    /// snapshot regardless of the peer's Index version. This closes the resend-once
    /// asymmetry: documents are resent from disk until the peer catches up, but Index
    /// node-creates ride a one-shot VV-delta, so a doc could otherwise arrive without
    /// its node. The full snapshot guarantees the node lands before the doc — exactly
    /// what the receive-side gate in `apply_doc.rs` relies on. Loro can't
    /// cross-reference the two separate documents, so this durability coupling is
    /// necessarily application-level.
    async fn export_index_updates(
        &self,
        their_index_version: &[u8],
        sent_new_doc_snapshot: bool,
    ) -> Option<Vec<u8>> {
        if sent_new_doc_snapshot {
            // A new-doc snapshot is riding along, so the receiver's Flow-2 gate
            // (`apply_doc.rs`) will hard-skip the doc unless its Index node arrives
            // in the SAME message. A silent export failure (→ `None`) would produce
            // an invisible no-sync: the doc ships but never materializes until a
            // later Index sync or boot reconcile heals it. Warn so that's visible.
            match self.index().export_snapshot() {
                Ok(updates) => Some(updates),
                Err(e) => {
                    warn!(
                        "prepare response: failed to export Index snapshot while \
                         shipping a new-doc snapshot; receiver will skip the new doc \
                         until its node arrives: {}",
                        e
                    );
                    None
                }
            }
        } else if !their_index_version.is_empty() {
            if let Ok(their_version) = loro::VersionVector::decode(their_index_version) {
                match self.index().export_updates(&their_version) {
                    Ok(updates) if !updates.is_empty() => Some(updates),
                    _ => None,
                }
            } else {
                // Invalid version — send a full snapshot.
                self.index().export_snapshot().ok()
            }
        } else {
            // They have no Index — send a full snapshot.
            self.index().export_snapshot().ok()
        }
    }
}
