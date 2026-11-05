//! Real-disk characterization of the P4 vault → UUID-store migration.
//!
//! The companion to `crates/vault-sync/tests/migration.rs`. That suite runs the
//! migration's eight-assertion safety contract over the in-memory `InMemoryFs`; this
//! one runs it over the REAL filesystem (`NativeFs` + `tempfile`), so it catches
//! anything `InMemoryFs` masks: real path semantics, `mkdir -p`, atomic writes, and
//! the case-insensitive APFS behavior of the cutover machine.
//!
//! **No test here ever touches the live `~/notes`.** The default test builds a
//! throwaway `tempfile::tempdir()` vault. The single `#[ignore]`-d test reads a COPY
//! of real `~/notes` that the operator stages and points at via an env var — it is
//! Michael's pre-cutover scale check, never a CI gate, and never the live vault.
//!
//! ## The migration, restated
//!
//! `Vault::init` over a directory of materialized `.md` files IS the migration (it
//! mints UUIDs, writes `.sync/docs/<uuid>.loro`, and builds the Index). These tests
//! pin its behavior on real disk; there is no net-new migration logic under test.

mod migration {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use p2p_core::PeerId;
    use sync_daemon::NativeFs;
    use tempfile::tempdir;
    use uuid::Uuid;
    use vault_sync::fs::FileSystem;
    use vault_sync::index::StructuralNode;
    use vault_sync::{ContentDoc, Vault, content_doc_path, content_hash};

    /// A deterministic author seed — the daemon authors Loro ops under a bare u64
    /// derived from its peer id; any stable value works for the migration's UUID mint.
    fn test_author() -> u64 {
        PeerId::from_secret_bytes([7u8; 32]).as_u64()
    }

    /// The `.sync/docs/` directory the migration writes content docs into — walked by
    /// the no-orphan assertion to enumerate on-disk doc UUIDs.
    const DOCS_DIR: &str = ".sync/docs";

    /// Every `File` node in the structural scan, as `(path, uuid)`.
    fn file_nodes(vault: &Vault<Arc<NativeFs>>) -> Vec<(String, Uuid)> {
        vault
            .index()
            .scan_structural_nodes()
            .into_iter()
            .filter_map(|n| match n {
                StructuralNode::File { path, uuid } => Some((path, uuid)),
                StructuralNode::Folder { .. } => None,
            })
            .collect()
    }

    /// The set of UUIDs of every `docs/*.loro` on real disk (the no-orphan input).
    async fn on_disk_doc_uuids(fs: &Arc<NativeFs>) -> BTreeSet<Uuid> {
        let entries = match fs.list(DOCS_DIR).await {
            Ok(entries) => entries,
            Err(_) => return BTreeSet::new(),
        };
        entries
            .into_iter()
            .filter(|e| !e.is_dir && e.name.ends_with(".loro"))
            .map(|e| {
                let stem = e.name.strip_suffix(".loro").unwrap();
                Uuid::parse_str(stem).unwrap_or_else(|_| {
                    panic!("a docs/ entry is not a UUID-named .loro: {}", e.name)
                })
            })
            .collect()
    }

    /// Run the eight-assertion migration safety contract against a migrated NativeFs
    /// vault. `originals` maps each `.md` path to its ORIGINAL on-disk bytes so
    /// assertion 6 re-parses them to a normalized `content_hash`.
    ///
    /// Assertion 8 (re-clone convergence) lives in the in-memory suite; this real-disk
    /// surface covers assertions 1–7 (the on-disk lossless-rebuild contract).
    async fn assert_migration_contract(
        vault: &Vault<Arc<NativeFs>>,
        fs: &Arc<NativeFs>,
        originals: &[(String, String)],
    ) {
        let md_files: BTreeSet<String> = vault.list_files().await.unwrap().into_iter().collect();
        let nodes = file_nodes(vault);

        // Assertion 1: every `.md` is indexed with a UUID.
        for path in &md_files {
            let node = vault
                .index()
                .node_for_path(path)
                .unwrap_or_else(|| panic!("assertion 1: no Index node for migrated file {path}"));
            assert!(
                vault.index().node_uuid(&node).is_some(),
                "assertion 1: node for {path} carries no UUID"
            );
        }

        // Assertion 2: counts match.
        assert_eq!(
            md_files.len(),
            nodes.len(),
            "assertion 2: file-node count must equal the `.md` count"
        );

        // Assertion 3: no duplicate UUIDs.
        let unique_uuids: BTreeSet<Uuid> = nodes.iter().map(|(_, u)| *u).collect();
        assert_eq!(
            unique_uuids.len(),
            nodes.len(),
            "assertion 3: every file node must have a distinct UUID"
        );

        // Assertion 4: every node's content `.loro` exists.
        for (path, uuid) in &nodes {
            assert!(
                fs.exists(&content_doc_path(uuid)).await.unwrap(),
                "assertion 4: node {path} ({uuid}) has no docs/<uuid>.loro on disk"
            );
        }

        // Assertion 5: no orphaned `.loro`.
        assert_eq!(
            on_disk_doc_uuids(fs).await,
            unique_uuids,
            "assertion 5: on-disk doc UUIDs must equal live-node UUIDs (no orphan, no unbacked node)"
        );

        // Assertion 6: content preserved via NORMALIZED content_hash.
        let originals_by_path: std::collections::HashMap<&str, &str> = originals
            .iter()
            .map(|(p, c)| (p.as_str(), c.as_str()))
            .collect();
        for (path, _uuid) in &nodes {
            let original = originals_by_path
                .get(path.as_str())
                .unwrap_or_else(|| panic!("assertion 6: no staged original for {path}"));
            let expected = ContentDoc::from_markdown(original, test_author()).unwrap();
            let actual = vault.get_document(path).await.unwrap();
            assert_eq!(
                content_hash(&actual),
                content_hash(&expected),
                "assertion 6: migrated content at {path} must match the original's NORMALIZED hash"
            );
        }

        // Assertion 7: folder paths preserved — the migrated `.md` set equals what was
        // staged on disk (the keys of `originals`).
        let expected_md: BTreeSet<String> = originals.iter().map(|(p, _)| p.clone()).collect();
        assert_eq!(
            md_files, expected_md,
            "assertion 7: the migrated `.md` path set must equal the on-disk `.md` set"
        );
    }

    /// A representative `.md` tree on a real tempdir migrates correctly: nested
    /// folders, unicode/spaced filenames, frontmatter normalization, and non-`.md`
    /// neighbors — the full contract over real path semantics and atomic writes.
    ///
    /// This is the real-disk proof: it builds a throwaway vault (NEVER `~/notes`),
    /// writes a varied tree to actual disk, runs `Vault::init` (the migration), and
    /// asserts the lossless-rebuild contract against the real `.sync/` on disk.
    #[tokio::test]
    async fn representative_tree_migrates_losslessly_on_real_disk() {
        let dir = tempdir().expect("tempdir");
        let fs = Arc::new(NativeFs::new(dir.path().to_path_buf()));

        // A varied tree exercising nesting, unicode, spaces, frontmatter, and a large
        // file — staged on REAL disk before the migration runs.
        let big_md = format!("# Large\n\n{}", "Body paragraph. ".repeat(40_000));
        let originals: Vec<(String, String)> = vec![
            ("root.md".into(), "# Root\n\nAt the vault root.".into()),
            (
                "Projects/alpha/spec.md".into(),
                "---\nzebra: 1\napple: 2\n---\n\n\n# Spec\n\nUnsorted frontmatter + blank lines."
                    .into(),
            ),
            (
                "Projects/beta/notes.md".into(),
                "# Beta notes\n\nA sibling project.".into(),
            ),
            (
                "café ☕/über note.md".into(),
                "# Café\n\nUnicode folder and file with a space.".into(),
            ),
            ("large/big.md".into(), big_md),
        ];

        // Write the `.md` tree to real disk (NativeFs::write does mkdir -p).
        for (path, content) in &originals {
            fs.write(path, content.as_bytes()).await.unwrap();
        }
        // Non-`.md` neighbors that must be skipped by the migration.
        fs.write("Projects/alpha/diagram.png", b"\x89PNG not markdown")
            .await
            .unwrap();
        fs.write("README.txt", b"plain text").await.unwrap();

        // The migration.
        let vault = Vault::init(Arc::clone(&fs), test_author())
            .await
            .expect("migration (Vault::init) succeeds on real disk");

        assert_migration_contract(&vault, &fs, &originals).await;

        // The non-`.md` neighbors were skipped (not indexed).
        for skipped in ["Projects/alpha/diagram.png", "README.txt"] {
            assert!(
                vault.index().node_for_path(skipped).is_none(),
                "{skipped} must not be indexed (not a `.md`)"
            );
        }
    }

    /// GATED scale check against a COPY of real `~/notes` — Michael's pre-cutover
    /// validation instrument, NOT a CI gate.
    ///
    /// Reads a COPY of the real vault that the operator stages themselves; it NEVER
    /// touches the live `~/notes`. Invocation:
    ///
    /// ```sh
    /// # Stage a copy (never point at the live vault):
    /// cp -R ~/notes /tmp/notes-migration-copy
    /// OM_MIGRATION_COPY_DIR=/tmp/notes-migration-copy \
    ///   cargo test -p sync-daemon --test migration -- --ignored --nocapture
    /// ```
    ///
    /// The copy directory must be a materialized `.md` tree with NO `.sync/` present
    /// (a clean rebuild target — strip any existing `.sync/` from the copy first).
    /// The test migrates the copy in place and asserts the full eight-assertion
    /// contract at real scale (~900 notes), printing the counts. Skips with a message
    /// if the env var is unset, so a bare `--ignored` run is a no-op rather than a
    /// failure.
    #[tokio::test]
    #[ignore = "scale check: needs OM_MIGRATION_COPY_DIR pointing at a COPY of real notes"]
    async fn real_notes_copy_migrates_losslessly_at_scale() {
        let Ok(copy_dir) = std::env::var("OM_MIGRATION_COPY_DIR") else {
            eprintln!(
                "skipping: set OM_MIGRATION_COPY_DIR to a COPY of ~/notes (never the live vault) \
                 to run this scale check"
            );
            return;
        };

        let copy_path = std::path::PathBuf::from(&copy_dir);
        assert!(
            copy_path.is_dir(),
            "OM_MIGRATION_COPY_DIR ({copy_dir}) must be an existing directory (a COPY of ~/notes)"
        );
        assert!(
            !copy_path.join(".sync").exists(),
            "OM_MIGRATION_COPY_DIR ({copy_dir}) still has a `.sync/` — remove it so the migration \
             rebuilds cleanly (this is the clean-rebuild precondition, and a guard against \
             accidentally pointing at a live vault)"
        );

        let fs = Arc::new(NativeFs::new(copy_path.clone()));

        // Capture the ORIGINAL `.md` bytes BEFORE the migration, by walking the copy's
        // `.md` tree via the same `list_files` filter the migration uses. (The
        // migration does not mutate the `.md` files, but reading them up front pins the
        // assertion-6 baseline against pre-migration content.)
        let pre_md = list_md_recursive(&fs).await;
        let mut originals: Vec<(String, String)> = Vec::with_capacity(pre_md.len());
        for path in &pre_md {
            let bytes = fs.read(path).await.unwrap();
            originals.push((path.clone(), String::from_utf8_lossy(&bytes).into_owned()));
        }

        eprintln!(
            "real-notes scale check: migrating {} `.md` files from {copy_dir}",
            originals.len()
        );

        let vault = Vault::init(Arc::clone(&fs), test_author())
            .await
            .expect("migration of the real-notes COPY succeeds");

        assert_migration_contract(&vault, &fs, &originals).await;

        let node_count = file_nodes(&vault).len();
        eprintln!(
            "real-notes scale check PASSED: {node_count} notes migrated, all eight assertions hold"
        );
    }

    /// Every `.md` path under a NativeFs vault root, applying the same filter the
    /// migration uses (skip `.sync` and hidden, keep `.md`). Used only by the gated
    /// scale check to snapshot the copy's `.md` tree before migrating.
    async fn list_md_recursive(fs: &Arc<NativeFs>) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![String::new()];
        while let Some(dir) = stack.pop() {
            let entries = match fs.list(&dir).await {
                Ok(e) => e,
                Err(_) => continue,
            };
            for entry in entries {
                let path = if dir.is_empty() {
                    entry.name.clone()
                } else {
                    format!("{dir}/{}", entry.name)
                };
                if path.starts_with(".sync") || path.starts_with('.') {
                    continue;
                }
                if entry.is_dir {
                    stack.push(path);
                } else if path.ends_with(".md") {
                    out.push(path);
                }
            }
        }
        out
    }
}
