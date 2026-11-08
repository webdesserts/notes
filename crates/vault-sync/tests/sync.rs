//! Convergence acceptance tests for the transport seam — the bulk of Phase 1's
//! proof that two replicas converge.
//!
//! These drive the public seam the way a consuming daemon will: `prepare_request`
//! to open a handshake, ship the bytes to a peer, and feed every inbound payload to
//! `process_message`. The headline properties are UUID identity (a move re-transfers
//! zero content — INV-1), same-document CRDT merge (INV-2), byte-identical
//! convergence (INV-4), the structural Flow-2 gate (INV-8), delete propagation
//! (INV-9), idempotent/out-of-order application (INV-10), the send-side node-first
//! coupling (S5), and the lossy-transport seam contract (§5).
//!
//! Everything runs against `InMemoryFs` — no test touches a real vault.
//!
//! The replica/handshake/edit helpers live in the shared [`common`] harness
//! (`tests/common/mod.rs`) so every test surface drives the seam identically.

use std::collections::HashMap;
use std::sync::Arc;

use vault_sync::{DocId, FileSystem, InMemoryFs, SyncMessage, Vault, content_doc_path};

mod common;
use common::*;

// ========================= AC-INV-1 — identity / zero-content move =========================

mod ac_inv_1_zero_content_move {
    use super::*;

    /// A move re-transfers ZERO document content: after A and B converge, A moves a
    /// document, and the next sync carries only the Index move-op (a `tree.mov`) —
    /// no document-content bytes for the moved doc — while its UUID stays stable on
    /// both replicas.
    #[tokio::test]
    async fn move_syncs_index_op_only_with_zero_content_bytes_and_stable_uuid() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        // A creates a document and both replicas converge on it.
        write_and_index(&a, &fs_a, "notes/topic.md", "# Topic\n\nBody text here.").await;
        full_sync(&a, &b).await;
        let original_uuid = uuid_at(&a, "notes/topic.md");
        assert_eq!(
            uuid_at(&b, "notes/topic.md"),
            original_uuid,
            "both replicas share the document's UUID after the initial sync"
        );

        // A moves the document: a pure-structural Index op plus the on-disk rename.
        move_file(&a, &fs_a, "notes/topic.md", "archive/topic.md").await;

        // Sync the move and inspect the actual wire payloads. The move changed A's
        // catalog digest, so the handshake takes the four-message digest-MISS path
        // (§6.2): A opens with its digest → B replies DigestMismatch (its VVs) → A
        // answers with a SyncExchange that CARRIES the move (A→B) → B replies a final
        // SyncResponse (B→A). The move now rides the SyncExchange, one leg later than the
        // old three-message flow, so the zero-content assertions move onto that leg.
        let request = a.prepare_request().await.unwrap();
        let mismatch_bytes = b.process_message(&request).await.unwrap().reply.unwrap();
        assert!(
            matches!(decode(&mismatch_bytes), SyncMessage::DigestMismatch { .. }),
            "the move changed A's digest, so B replies DigestMismatch, got {:?}",
            decode(&mismatch_bytes)
        );

        // A→B: the SyncExchange that carries the move (the leg the move now rides).
        let exchange_bytes = a
            .process_message(&mismatch_bytes)
            .await
            .unwrap()
            .reply
            .unwrap();
        let exchange = decode(&exchange_bytes);

        // B→A: B applies the move and replies the final SyncResponse.
        let final_bytes = b
            .process_message(&exchange_bytes)
            .await
            .unwrap()
            .reply
            .unwrap();
        let final_msg = decode(&final_bytes);
        a.process_message(&final_bytes).await.unwrap();

        // The move rides the Index delta, not as document content. NEITHER the exchange
        // carrying the move (A→B) nor B's final response (B→A) carries any
        // document-content bytes for the moved doc — the headline INV-1 guarantee.
        assert_eq!(
            document_content_bytes(&exchange),
            0,
            "the exchange carrying the move sends zero document-content bytes"
        );
        assert_eq!(
            document_content_bytes(&final_msg),
            0,
            "B's final response carries zero document-content bytes for a pure move"
        );
        // The Index move-op DID cross — on the SyncExchange (A→B), the leg that carries
        // the move under the four-message flow.
        match &exchange {
            SyncMessage::SyncExchange { response, .. } => assert!(
                response.index_updates.is_some(),
                "the move ships an Index delta (the tree.mov op) on the exchange"
            ),
            other => panic!("expected a SyncExchange carrying the move, got {other:?}"),
        }

        // B converged on the move with the SAME UUID — identity is stable.
        assert!(
            b.index().node_for_path("notes/topic.md").is_none(),
            "B's old path is vacated after the move"
        );
        assert_eq!(
            uuid_at(&b, "archive/topic.md"),
            original_uuid,
            "the moved document keeps its UUID on B"
        );
        assert_eq!(
            uuid_at(&a, "archive/topic.md"),
            original_uuid,
            "the moved document keeps its UUID on A"
        );

        // The content `.loro` was never relocated — it is addressed by the stable
        // UUID, so the same file backs the document before and after the move.
        let loro = content_doc_path(&original_uuid);
        assert!(
            fs_b.exists(&loro).await.unwrap(),
            "B still has the same <uuid>.loro"
        );
        // B's old-path `.md` is gone; the new-path `.md` is present.
        assert!(!fs_b.exists("notes/topic.md").await.unwrap());
        assert!(fs_b.exists("archive/topic.md").await.unwrap());

        // The re-materialized `.md` at the new path carries the CORRECT content,
        // rendered from the path-independent `.loro` (not empty or corrupt). Zero
        // content crossed the wire, so this proves the receiver rendered it locally.
        let moved_md = String::from_utf8(read_md(&fs_b, "archive/topic.md").await).unwrap();
        assert!(
            moved_md.contains("Body text here."),
            "the re-materialized .md has the document's body, not empty/corrupt content: {moved_md:?}"
        );
    }
}

// ========================= AC-INV-2 — same-doc concurrent merge (e2e) =========================

mod ac_inv_2_concurrent_merge {
    use super::*;

    /// Two replicas edit DIFFERENT paragraphs of the same document offline, then
    /// cross-sync: both edits are present, there is no interleaving, and the merged
    /// version vector has one entry per author (proof the edits were independent).
    #[tokio::test]
    async fn concurrent_edits_to_one_doc_merge_without_loss_or_interleaving() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        // Establish a shared base document on both replicas.
        write_and_index(&a, &fs_a, "doc.md", "para one\n\npara two\n").await;
        full_sync(&a, &b).await;

        // Offline divergent edits: A rewrites the first paragraph, B the second.
        write_and_index(&a, &fs_a, "doc.md", "para one EDITED BY A\n\npara two\n").await;
        write_and_index(&b, &fs_b, "doc.md", "para one\n\npara two EDITED BY B\n").await;

        // Cross-sync to quiescence.
        sync_both_ways(&a, &b).await;

        // Both replicas converge to byte-identical markdown containing BOTH edits.
        let md_a = String::from_utf8(read_md(&fs_a, "doc.md").await).unwrap();
        let md_b = String::from_utf8(read_md(&fs_b, "doc.md").await).unwrap();
        assert_eq!(
            md_a, md_b,
            "both replicas converge to byte-identical content"
        );
        assert!(md_a.contains("EDITED BY A"), "A's edit survived: {md_a:?}");
        assert!(md_a.contains("EDITED BY B"), "B's edit survived: {md_a:?}");

        // The merged document's version vector has one entry per device author —
        // proof the two replicas authored under distinct Loro peer ids (no shared-
        // author OpId collision).
        let doc = a.get_document("doc.md").await.unwrap();
        assert_eq!(
            doc.version().iter().count(),
            2,
            "merged version vector has one entry per device author"
        );
    }
}

// ========================= AC-INV-4 — convergence (basic) =========================

mod ac_inv_4_convergence {
    use super::*;

    /// Two replicas with a handful of divergent edits, moves, and deletes pumped to
    /// quiescence converge to identical materialized state — same paths, byte-
    /// identical `.md` — and identical merged version vectors.
    #[tokio::test]
    async fn divergent_edits_moves_deletes_converge_to_identical_state() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        // A shared starting set.
        write_and_index(&a, &fs_a, "keep.md", "keep me\n").await;
        write_and_index(&a, &fs_a, "move-me.md", "move target\n").await;
        write_and_index(&a, &fs_a, "delete-me.md", "doomed\n").await;
        full_sync(&a, &b).await;

        // Capture the doomed document's UUID before the delete so we can assert its
        // content `.loro` (UUID-addressed) is cleaned up on both replicas afterward.
        let delete_me_uuid = uuid_at(&b, "delete-me.md");

        // Divergent ops across the two replicas.
        // A edits `keep.md` and moves `move-me.md`.
        write_and_index(&a, &fs_a, "keep.md", "keep me, edited\n").await;
        move_file(&a, &fs_a, "move-me.md", "moved/here.md").await;
        // B creates a new document and deletes `delete-me.md`. A locally-originated
        // delete tombstones the catalog node (`delete_node` is fs-agnostic by design)
        // and the originator cleans BOTH on-disk files — the `.md` and its UUID-
        // addressed `.loro` — exactly as a daemon's local-delete handler does. (The
        // RECEIVE side, where a peer's tombstone arrives, cleans the `.loro` itself
        // via the Index-delta path; that propagation is asserted on A below.)
        write_and_index(&b, &fs_b, "b-only.md", "made by B\n").await;
        b.index().delete_node("delete-me.md").unwrap();
        b.save_index().await.unwrap();
        fs_b.delete("delete-me.md").await.unwrap();
        fs_b.delete(&content_doc_path(&delete_me_uuid))
            .await
            .unwrap();

        // Pump both directions until quiescent (two rounds settle a delete +
        // create + move + edit fan-out).
        sync_both_ways(&a, &b).await;
        sync_both_ways(&a, &b).await;

        // Same set of live paths on both replicas.
        let mut files_a = a.list_files().await.unwrap();
        let mut files_b = b.list_files().await.unwrap();
        files_a.sort();
        files_b.sort();
        assert_eq!(
            files_a, files_b,
            "both replicas hold the same set of live paths"
        );
        assert_eq!(
            files_a,
            vec![
                "b-only.md".to_string(),
                "keep.md".to_string(),
                "moved/here.md".to_string(),
            ],
            "the converged path set reflects every op (edit/move/create/delete)"
        );

        // Byte-identical `.md` for every live path.
        for path in &files_a {
            let md_a = read_md(&fs_a, path).await;
            let md_b = read_md(&fs_b, path).await;
            assert_eq!(md_a, md_b, "{path} is byte-identical on both replicas");
        }

        // The deleted document is gone on both replicas — both the `.md` AND its
        // UUID-addressed content `.loro`.
        let deleted_loro = content_doc_path(&delete_me_uuid);
        assert!(!fs_a.exists("delete-me.md").await.unwrap());
        assert!(!fs_b.exists("delete-me.md").await.unwrap());
        assert!(
            !fs_a.exists(&deleted_loro).await.unwrap(),
            "A's deleted-doc .loro is cleaned up"
        );
        assert!(
            !fs_b.exists(&deleted_loro).await.unwrap(),
            "B's deleted-doc .loro is cleaned up"
        );

        // Identical merged Index version vectors — the catalogs converged.
        assert_eq!(
            a.index().state_vv(),
            b.index().state_vv(),
            "both replicas' Index version vectors converge"
        );
    }
}

// ========================= AC-INV-8 — the Flow-2 gate =========================

mod ac_inv_8_flow2_gate {
    use super::*;

    /// A `DocUpdate` for a UUID whose Index node has NOT arrived is held: no `.md`
    /// materializes. Once the node arrives (a full sync ships it with the doc), the
    /// document then materializes. The gate is structural — no node ⇒ no path ⇒
    /// nowhere to write.
    #[tokio::test]
    async fn docupdate_without_node_is_held_then_materializes_when_node_lands() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        // A creates a document that B has never heard of.
        write_and_index(&a, &fs_a, "secret.md", "hidden content\n").await;
        let uuid = DocId(uuid_at(&a, "secret.md"));

        // A pushes ONLY the document (a lone real-time DocUpdate, no node).
        let push = a.prepare_doc_update(uuid).await.unwrap().unwrap();
        let outcome = b.process_message(&push).await.unwrap();

        // The gate held it: nothing materialized on B.
        assert!(
            outcome.modified.is_empty(),
            "the held update reports no modification"
        );
        assert!(
            !fs_b.exists("secret.md").await.unwrap(),
            "no .md materializes for a document whose node has not arrived"
        );
        assert!(
            !fs_b.exists(&content_doc_path(&uuid.0)).await.unwrap(),
            "no <uuid>.loro is written for a held update"
        );

        // Now a full sync ships the node alongside the document (S5/C3 coupling).
        full_sync(&a, &b).await;

        // It materializes now that the node has landed.
        assert!(
            fs_b.exists("secret.md").await.unwrap(),
            "the document materializes once its Index node arrives"
        );
        assert_eq!(
            uuid_at(&b, "secret.md"),
            uuid.0,
            "the materialized document carries the shared UUID"
        );
        let md = String::from_utf8(read_md(&fs_b, "secret.md").await).unwrap();
        assert!(
            md.contains("hidden content"),
            "the materialized content is correct"
        );
    }
}

// ========================= AC-INV-9 — delete propagation =========================

mod ac_inv_9_delete_propagation {
    use super::*;

    /// A delete on A propagates to B: B's `.md` and `docs/<uuid>.loro` are gone and
    /// the node is tombstoned.
    #[tokio::test]
    async fn delete_propagates_removing_md_and_loro_and_tombstoning_node() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        write_and_index(&a, &fs_a, "doomed.md", "delete me\n").await;
        full_sync(&a, &b).await;
        let uuid = uuid_at(&b, "doomed.md");
        assert!(
            fs_b.exists("doomed.md").await.unwrap(),
            "B has the file before the delete"
        );

        // A deletes the document (Index tombstone + fs cleanup) and syncs.
        a.index().delete_node("doomed.md").unwrap();
        a.save_index().await.unwrap();
        fs_a.delete("doomed.md").await.unwrap();
        sync_both_ways(&a, &b).await;

        // B's file, content `.loro`, and live node are all gone.
        assert!(
            !fs_b.exists("doomed.md").await.unwrap(),
            "B's .md is removed"
        );
        assert!(
            !fs_b.exists(&content_doc_path(&uuid)).await.unwrap(),
            "B's <uuid>.loro is removed"
        );
        assert!(
            b.index().node_for_path("doomed.md").is_none(),
            "B's node is tombstoned (no live node at the path)"
        );
    }

    /// A concurrent edit-vs-delete converges to the tombstone (Registry-derived
    /// liveness, INV-9): A deletes while B edits the same document offline; after
    /// syncing, both replicas agree the document is gone — the edit does not
    /// resurrect it.
    #[tokio::test]
    async fn concurrent_edit_vs_delete_converges_to_tombstone() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        write_and_index(&a, &fs_a, "contested.md", "original\n").await;
        full_sync(&a, &b).await;

        // Offline: A deletes, B edits the same document.
        a.index().delete_node("contested.md").unwrap();
        a.save_index().await.unwrap();
        fs_a.delete("contested.md").await.unwrap();
        write_and_index(&b, &fs_b, "contested.md", "edited while A deleted\n").await;

        // Converge.
        sync_both_ways(&a, &b).await;
        sync_both_ways(&a, &b).await;

        // The tombstone wins on BOTH replicas — the edit did not resurrect the file.
        assert!(
            b.index().node_for_path("contested.md").is_none(),
            "B agrees the document is deleted (tombstone wins over the concurrent edit)"
        );
        assert!(
            !fs_b.exists("contested.md").await.unwrap(),
            "B's file is gone"
        );
        assert!(
            a.index().node_for_path("contested.md").is_none(),
            "A's deletion stands"
        );
        assert!(
            !fs_a.exists("contested.md").await.unwrap(),
            "A's file stays gone"
        );
    }
}

// ========================= AC-INV-10 — idempotent / out-of-order =========================

mod ac_inv_10_idempotent_ordered {
    use super::*;

    /// Applying the same payload twice yields identical state with no error
    /// (idempotent).
    #[tokio::test]
    async fn applying_the_same_payload_twice_is_idempotent() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        write_and_index(&a, &fs_a, "doc.md", "content\n").await;
        full_sync(&a, &b).await;
        let md_after_first = read_md(&fs_b, "doc.md").await;
        let vv_after_first = b.get_document("doc.md").await.unwrap().version();

        // Replay the entire handshake again — every payload re-applied.
        full_sync(&a, &b).await;

        let md_after_second = read_md(&fs_b, "doc.md").await;
        let vv_after_second = b.get_document("doc.md").await.unwrap().version();
        assert_eq!(
            md_after_first, md_after_second,
            "re-applying changes nothing on disk"
        );
        assert_eq!(
            vv_after_first, vv_after_second,
            "re-applying changes no version vector"
        );
    }

    /// A 3-op causal chain delivered in EVERY ordering converges to identical state.
    /// Out-of-order deltas park on their unsatisfied dependencies (Loro's
    /// `ImportStatus.pending`) and apply once the missing op lands, so the converged
    /// result is independent of delivery order.
    #[tokio::test]
    async fn out_of_order_causal_chain_converges_in_every_ordering() {
        // Build a 3-edit causal chain on a source vault. Capture the step-0 snapshot
        // (a self-contained base with the document's UUID) and the three incremental
        // deltas (v0→v1, v1→v2, v2→v3), each depending on the prior.
        let fs_src = Arc::new(InMemoryFs::new());
        let src = Vault::init(Arc::clone(&fs_src), AUTHOR_A).await.unwrap();
        write_and_index(&src, &fs_src, "chain.md", "step 0\n").await;
        let uuid = DocId(uuid_at(&src, "chain.md"));

        // A complete, self-contained snapshot of the document at step 0 (the base the
        // deltas attach to) plus the Index snapshot that carries its node.
        let step0_snapshot = src
            .get_document("chain.md")
            .await
            .unwrap()
            .export_snapshot()
            .unwrap();
        let index_snapshot = src.index().export_snapshot().unwrap();
        let vv0 = src.get_document("chain.md").await.unwrap().version();

        write_and_index(&src, &fs_src, "chain.md", "step 0\nstep 1\n").await;
        let vv1 = src.get_document("chain.md").await.unwrap().version();
        write_and_index(&src, &fs_src, "chain.md", "step 0\nstep 1\nstep 2\n").await;
        let vv2 = src.get_document("chain.md").await.unwrap().version();
        write_and_index(
            &src,
            &fs_src,
            "chain.md",
            "step 0\nstep 1\nstep 2\nstep 3\n",
        )
        .await;

        let doc = src.get_document("chain.md").await.unwrap();
        let deltas = [
            doc.export_updates(&vv0).unwrap(),
            doc.export_updates(&vv1).unwrap(),
            doc.export_updates(&vv2).unwrap(),
        ];
        let final_md = String::from_utf8(read_md(&fs_src, "chain.md").await).unwrap();

        let orderings = [
            [0usize, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0],
        ];

        for order in orderings {
            // A fresh receiver. Deliver the node (Index snapshot, registry-before-
            // documents) then the step-0 base, satisfying the Flow-2 gate so only
            // causal ordering of the three deltas is under test.
            let fs_rx = Arc::new(InMemoryFs::new());
            let rx = Vault::init(Arc::clone(&fs_rx), AUTHOR_B).await.unwrap();

            let bootstrap = SyncMessage::SyncResponse {
                index_updates: Some(index_snapshot.clone()),
                document_updates: HashMap::from([(uuid, step0_snapshot.clone())]),
            };
            rx.process_message(&bincode::serialize(&bootstrap).unwrap())
                .await
                .unwrap();
            assert_eq!(
                String::from_utf8(read_md(&fs_rx, "chain.md").await).unwrap(),
                "step 0\n",
                "receiver starts at the step-0 base before the deltas"
            );

            // Deliver the three deltas in the permuted order.
            for &i in &order {
                let msg = SyncMessage::DocUpdate {
                    uuid,
                    data: deltas[i].clone(),
                };
                rx.process_message(&bincode::serialize(&msg).unwrap())
                    .await
                    .unwrap();
            }

            let got = String::from_utf8(read_md(&fs_rx, "chain.md").await).unwrap();
            assert_eq!(
                got, final_md,
                "out-of-order delivery {order:?} must converge to the final state"
            );
        }
    }
}

// ========================= AC-S5 — send-side node-first =========================

mod ac_s5_send_side_node_first {
    use super::*;

    /// A lone `prepare_doc_update` push for a brand-new document whose node B lacks
    /// is hard-skipped (no materialize); a subsequent full sync ships the node AND
    /// the document, and both land — the full-sync backstop recovers the held doc.
    #[tokio::test]
    async fn lone_new_doc_push_is_skipped_then_recovered_by_full_sync() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        write_and_index(&a, &fs_a, "fresh.md", "brand new\n").await;
        let uuid = DocId(uuid_at(&a, "fresh.md"));

        // The lone push is hard-skipped (S5): no node, no materialize.
        let push = a.prepare_doc_update(uuid).await.unwrap().unwrap();
        b.process_message(&push).await.unwrap();
        assert!(
            !fs_b.exists("fresh.md").await.unwrap(),
            "a lone new-doc push does not materialize without its node"
        );
        assert!(
            b.index().find_node_by_uuid(&uuid.0).is_none(),
            "B has no node for the pushed document yet"
        );

        // A full sync ships the node WITH the document (the C3 coupling: any new-doc
        // snapshot forces the full Index snapshot to ride along).
        full_sync(&a, &b).await;
        assert!(
            fs_b.exists("fresh.md").await.unwrap(),
            "the document materializes once a full sync ships its node"
        );
        assert_eq!(
            uuid_at(&b, "fresh.md"),
            uuid.0,
            "B's node carries the shared UUID"
        );
    }
}

// ========================= AC-§5 — seam contract under a lossy transport =========================

mod ac_seam_lossy_transport {
    use super::*;

    /// A lossy transport that drops, duplicates, and reorders payloads still
    /// converges: the seam's `process_message` tolerates re-delivery and missing
    /// messages because every exchange recomputes from version vectors. The test
    /// also exercises that `process_message` is the only inbound-data fs mutator —
    /// the receiver's files only ever change as a result of a `process_message` call.
    #[tokio::test]
    async fn lossy_drop_dup_reorder_converges_and_process_message_is_sole_mutator() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        // A starting set on A, plus an independent doc on B.
        write_and_index(&a, &fs_a, "one.md", "one\n").await;
        write_and_index(&a, &fs_a, "two.md", "two\n").await;
        write_and_index(&b, &fs_b, "b-side.md", "b side\n").await;

        // A deliberately lossy round: capture each handshake message and re-deliver
        // it under drop/dup/reorder, asserting B's files change ONLY via
        // process_message (we never write to fs_b directly).
        //
        // Round 1: A→B request, but B's exchange reply is "dropped" (not delivered
        // to A) AND duplicated to B itself (a nonsensical reorder B must tolerate).
        let req1 = a.prepare_request().await.unwrap();
        let exch1 = b.process_message(&req1).await.unwrap().reply.unwrap();
        // Duplicate-deliver B's own exchange back to B (it must not corrupt B).
        let _ = b.process_message(&exch1).await; // tolerated (may error-or-noop, never panic)
        // Drop exch1 from A (simulate loss): A never sees it.

        // Round 2: a fresh, complete handshake A→B (recovers the dropped round).
        full_sync(&a, &b).await;
        // Duplicate the whole handshake (every payload re-applied).
        full_sync(&a, &b).await;

        // Round 3: the reverse direction, also duplicated.
        full_sync(&b, &a).await;
        full_sync(&b, &a).await;

        // Despite drops/dups/reorders, both replicas converge to the same live set
        // and byte-identical content.
        let mut files_a = a.list_files().await.unwrap();
        let mut files_b = b.list_files().await.unwrap();
        files_a.sort();
        files_b.sort();
        assert_eq!(
            files_a, files_b,
            "lossy transport still converges the path set"
        );
        assert_eq!(
            files_a,
            vec![
                "b-side.md".to_string(),
                "one.md".to_string(),
                "two.md".to_string()
            ],
            "every document converged across the lossy channel"
        );
        for path in &files_a {
            assert_eq!(
                read_md(&fs_a, path).await,
                read_md(&fs_b, path).await,
                "{path} converges byte-identically over the lossy channel"
            );
        }
    }
}


// ========================= 3.J Phase 0.3 — key-rotation re-pair preserves data =========================

mod key_rotation_repair_preserves_data {
    use super::*;
    use p2p_core::allowlist::{AllowlistError, AllowlistStorage, InMemoryAllowlist};
    use p2p_core::PeerId;

    /// Revoking a device is an allowlist operation only: it never deletes vault
    /// files or Loro state. A revoked device that rotates its key and re-pairs
    /// merges all pre-removal and offline edits without loss or duplication.
    #[tokio::test]
    async fn revoked_device_rotating_its_key_re_pairs_without_data_loss() {
        let (a, b, fs_a, fs_b) = two_vaults().await;

        // Pair B's initial identity into A's allowlist.
        let allowlist = InMemoryAllowlist::new();
        let old_b_id = PeerId::generate();
        allowlist.add_peer(old_b_id, "device-b").await.unwrap();
        assert!(allowlist.is_allowed(&old_b_id).await.unwrap());

        // Shared base document, converged on both replicas.
        write_and_index(&a, &fs_a, "shared.md", "para one\n\npara two\n").await;
        full_sync(&a, &b).await;

        // Revoke B. This touches the allowlist only.
        allowlist.remove_peer(&old_b_id).await.unwrap();
        assert!(!allowlist.is_allowed(&old_b_id).await.unwrap());
        assert!(
            allowlist.list_peers().await.unwrap().iter().any(|p| p.node_id == old_b_id),
            "the revoked peer stays as a tombstone"
        );

        // Re-adding the revoked identity must fail loudly, not silently no-op.
        let err = allowlist.add_peer(old_b_id, "device-b").await.unwrap_err();
        assert!(
            matches!(err, AllowlistError::PeerRevoked(id) if id == old_b_id),
            "re-adding a revoked peer returns PeerRevoked, got {err:?}"
        );

        // Revocation must not have touched data on either replica.
        assert_eq!(read_md_str(&fs_a, "shared.md").await, "para one\n\npara two\n");
        assert_eq!(read_md_str(&fs_b, "shared.md").await, "para one\n\npara two\n");

        // Offline divergent edits while disconnected.
        write_and_index(&a, &fs_a, "shared.md", "para one EDITED BY A\n\npara two\n").await;
        write_and_index(&a, &fs_a, "from-a.md", "from a\n").await;
        write_and_index(&b, &fs_b, "shared.md", "para one\n\npara two EDITED BY B\n").await;
        write_and_index(&b, &fs_b, "from-b.md", "from b\n").await;

        // B rotates its device identity and is re-paired under the new key.
        let new_b_id = PeerId::generate();
        assert_ne!(new_b_id, old_b_id, "rotation yields a distinct identity");
        allowlist.add_peer(new_b_id, "device-b-rotated").await.unwrap();
        assert!(allowlist.is_allowed(&new_b_id).await.unwrap());

        // Re-pair and converge.
        sync_both_ways(&a, &b).await;

        let md_a = read_md_str(&fs_a, "shared.md").await;
        let md_b = read_md_str(&fs_b, "shared.md").await;
        assert_eq!(md_a, md_b, "both replicas converge byte-identically");
        assert!(md_a.contains("EDITED BY A"), "A edit survived: {md_a:?}");
        assert!(md_a.contains("EDITED BY B"), "B edit survived: {md_a:?}");
        assert_eq!(read_md_str(&fs_a, "from-a.md").await, "from a\n");
        assert_eq!(read_md_str(&fs_b, "from-b.md").await, "from b\n");
        assert_eq!(read_md_str(&fs_a, "from-b.md").await, "from b\n");
        assert_eq!(read_md_str(&fs_b, "from-a.md").await, "from a\n");
    }
}
