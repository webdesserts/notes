//! The canonical replace-behavior fixture, exercised through the notes-core
//! kernel plus the hash-free section resolution it composes with.
//!
//! This is the pure layer of the fixture contract: the Memory MCP adapter
//! (`crates/memory`) and the Autonomy native adapter (via a byte-identical
//! snapshot of the same JSON file) must run these SAME cases through their
//! exposed tool adapters. See `test-fixtures/replace-behavior.json` for the
//! pinned semantics.

use notes_core::{ReplacementEdit, apply_replacements, resolve_section_for_edit, splice_section};
use serde::Deserialize;

#[derive(Deserialize)]
struct Fixture {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    id: String,
    content: String,
    #[serde(default)]
    section: Option<String>,
    edits: Vec<EditCase>,
    expect: String,
    #[serde(default)]
    error: Option<String>,
    #[serde(rename = "expectedContent")]
    expected_content: String,
}

#[derive(Deserialize)]
struct EditCase {
    #[serde(rename = "oldText")]
    old_text: String,
    #[serde(rename = "newText")]
    new_text: String,
}

const FIXTURE_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/test-fixtures/replace-behavior.json"
);

#[test]
fn canonical_fixture_cases_behave_through_kernel_and_section_resolution() {
    let raw = std::fs::read_to_string(FIXTURE_PATH)
        .unwrap_or_else(|e| panic!("canonical fixture must be readable at {FIXTURE_PATH}: {e}"));
    let fixture: Fixture = serde_json::from_str(&raw).expect("canonical fixture must parse");

    assert!(
        !fixture.cases.is_empty(),
        "canonical fixture must contain cases"
    );

    for case in &fixture.cases {
        match case.expect.as_str() {
            "ok" => {
                assert!(
                    case.error.is_none(),
                    "case {} is an ok case and must not declare an error kind",
                    case.id
                );
                // Resolve the scope once (mirroring the adapter contract), apply
                // the batch progressively in the selected scope, splice once.
                let final_content = match &case.section {
                    None => apply_replacements(&case.content, &edits_of(case))
                        .unwrap_or_else(|e| panic!("case {} should succeed: {e:?}", case.id)),
                    Some(path) => {
                        let resolved = resolve_section_for_edit(&case.content, path)
                            .unwrap_or_else(|e| panic!("case {} should resolve: {e:?}", case.id));
                        let modified_section =
                            apply_replacements(&resolved.section_content, &edits_of(case))
                                .unwrap_or_else(|e| {
                                    panic!("case {} should succeed: {e:?}", case.id)
                                });
                        splice_section(
                            &case.content,
                            resolved.start_line,
                            resolved.end_line,
                            &modified_section,
                        )
                    }
                };
                assert_eq!(
                    final_content, case.expected_content,
                    "case {}: full-note bytes after the write must match exactly",
                    case.id
                );
            }
            "error" => {
                let kind = case
                    .error
                    .as_deref()
                    .unwrap_or_else(|| panic!("error case {} must declare an error kind", case.id));

                // Section-scope resolution failures happen before any edit runs.
                if kind == "section-not-found" || kind == "section-ambiguous" {
                    let path = case
                        .section
                        .as_deref()
                        .expect("scoped case needs a section");
                    let err = resolve_section_for_edit(&case.content, path)
                        .expect_err("case must refuse to resolve");
                    match (kind, err) {
                        ("section-not-found", notes_core::SectionWriteError::NotFound { .. }) => {}
                        ("section-ambiguous", notes_core::SectionWriteError::Ambiguous { .. }) => {}
                        (k, other) => panic!(
                            "case {}: expected {k}, got {:?}",
                            case.id,
                            error_kind_of(&other)
                        ),
                    }
                    assert_eq!(
                        case.content, case.expected_content,
                        "case {}: a refused scope must leave the note byte-for-byte unchanged",
                        case.id
                    );
                    continue;
                }

                // Edit-validation failures: the batch is refused as a whole.
                // Resolve the scope if scoped, then run the batch; assert the
                // failure and that the input bytes (what an adapter would have
                // to write) equal the expected unchanged bytes.
                let scope_content = match &case.section {
                    None => case.content.clone(),
                    Some(path) => {
                        let resolved = resolve_section_for_edit(&case.content, path)
                            .unwrap_or_else(|e| panic!("case {} should resolve: {e:?}", case.id));
                        resolved.section_content
                    }
                };
                let err = apply_replacements(&scope_content, &edits_of(case))
                    .expect_err("error case must not validate");
                match (kind, &err.error) {
                    ("not-found", notes_core::ReplacementError::NotFound { .. }) => {}
                    ("ambiguous", notes_core::ReplacementError::Ambiguous { .. }) => {}
                    (k, other) => panic!("case {}: expected {k}, got {other:?}", case.id),
                }
                assert_eq!(
                    case.content, case.expected_content,
                    "case {}: a refused batch must leave the note byte-for-byte unchanged",
                    case.id
                );
            }
            other => panic!("case {}: unknown expect kind {other}", case.id),
        }
    }
}

fn edits_of(case: &Case) -> Vec<ReplacementEdit> {
    case.edits
        .iter()
        .map(|e| ReplacementEdit {
            old_text: e.old_text.clone(),
            new_text: e.new_text.clone(),
        })
        .collect()
}

fn error_kind_of(err: &notes_core::SectionWriteError) -> &'static str {
    match err {
        notes_core::SectionWriteError::NotFound { .. } => "section-not-found",
        notes_core::SectionWriteError::Ambiguous { .. } => "section-ambiguous",
        notes_core::SectionWriteError::HashMismatch { .. } => "hash-mismatch",
    }
}
