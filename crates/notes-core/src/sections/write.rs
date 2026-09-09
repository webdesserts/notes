//! Section write resolution: the helper `write_note` uses to locate an
//! existing section fresh, verify its hash, and splice modified content
//! back into the full file (the create-side counterpart, for sections that
//! don't exist yet, is `super::create`).

use super::outline::{build_outline, extract_section};
use super::path::{SectionResolveError, resolve_section};
use crate::storage::ContentHash;

/// Error resolving or hash-verifying a section for a write.
///
/// No variant carries the current hash - matching `StorageError::HashMismatch`'s
/// convention that surfacing it would let a caller retry the write blind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SectionWriteError {
    /// No section's path suffix-matched the input.
    NotFound { path: String },
    /// 2+ sections' paths suffix-matched the input.
    Ambiguous {
        path: String,
        candidates: Vec<String>,
    },
    /// The section's current content hash doesn't match what the caller expected.
    HashMismatch { path: String },
}

/// A section resolved and hash-verified for a write, ready to be edited and
/// spliced back in.
#[derive(Debug)]
pub struct ResolvedSectionWrite {
    /// First line of the section in the full file, 1-indexed, inclusive.
    pub start_line: usize,
    /// Last line of the section in the full file, 1-indexed, inclusive.
    pub end_line: usize,
    /// The section's raw extracted content (per the Extraction contract - the
    /// exact string `extract_section` returns, nothing prepended/appended).
    pub section_content: String,
}

/// Build a fresh outline of `full_content`, resolve `path` against it, extract
/// the matched section, and verify its hash against `expected_hash`.
///
/// This is the single resolution entry point for editing an existing
/// section - `write_note`'s section-scoped edit branch calls this rather
/// than re-implementing matching. Resolving fresh (not against a
/// caller-cached outline) is what
/// makes "the section moved" safe: a section is re-located by path at write
/// time, so shifted line numbers from an earlier edit don't matter as long as
/// the target section's own content hasn't changed.
pub fn resolve_section_for_write(
    full_content: &str,
    path: &str,
    expected_hash: &str,
) -> Result<ResolvedSectionWrite, SectionWriteError> {
    // The outline is only needed to locate the section; it doesn't need to
    // outlive this call, so it's fine to build and drop it here rather than
    // threading a lifetime through the return type.
    let outline = build_outline(full_content);
    let section = resolve_section(&outline, path).map_err(|e| match e {
        SectionResolveError::NotFound { path } => SectionWriteError::NotFound { path },
        SectionResolveError::Ambiguous { path, candidates } => {
            SectionWriteError::Ambiguous { path, candidates }
        }
    })?;

    let section_content = extract_section(full_content, section);
    let actual_hash = ContentHash::from_content(&section_content);
    if actual_hash.as_str() != expected_hash {
        return Err(SectionWriteError::HashMismatch {
            path: section.path.clone(),
        });
    }

    Ok(ResolvedSectionWrite {
        start_line: section.start_line,
        end_line: section.end_line,
        section_content,
    })
}

/// Hash-free counterpart to [`resolve_section_for_write`] for replacement
/// tools that take no caller hash (exact text matching is their guard, and
/// the adapter's own whole-file fresh read guards the write).
///
/// Resolves `path` against a fresh outline of `full_content` and extracts the
/// matched section, without any hash verification:
/// [`SectionWriteError::HashMismatch`] is structurally unreachable here (the
/// error type is shared with the hash-verified path so adapters can map all
/// variants through one match arm).
///
/// The resolved region is the *only* region a scoped replacement may touch:
/// adapters must match edits inside `section_content` and splice the result
/// back once via [`splice_section`], never re-resolving the scope per edit
/// and never touching sibling sections.
pub fn resolve_section_for_edit(
    full_content: &str,
    path: &str,
) -> Result<ResolvedSectionWrite, SectionWriteError> {
    let outline = build_outline(full_content);
    let section = resolve_section(&outline, path).map_err(|e| match e {
        SectionResolveError::NotFound { path } => SectionWriteError::NotFound { path },
        SectionResolveError::Ambiguous { path, candidates } => {
            SectionWriteError::Ambiguous { path, candidates }
        }
    })?;

    let section_content = extract_section(full_content, section);
    Ok(ResolvedSectionWrite {
        start_line: section.start_line,
        end_line: section.end_line,
        section_content,
    })
}

/// Replace the inclusive `start_line..=end_line` range (1-indexed, in the same
/// `full_content.split('\n')` coordinate system as [`extract_section`]) with
/// `new_section_content`, and rejoin with `"\n"`.
///
/// A no-op splice (passing back the exact content that was extracted) must
/// reproduce `full_content` byte-for-byte - this is what lets an edit to one
/// section leave every sibling section untouched.
pub fn splice_section(
    full_content: &str,
    start_line: usize,
    end_line: usize,
    new_section_content: &str,
) -> String {
    let mut lines: Vec<&str> = full_content.split('\n').collect();
    let start = start_line - 1;
    let end = end_line; // exclusive upper bound for splice

    let replacement_lines: Vec<&str> = new_section_content.split('\n').collect();
    lines.splice(start..end, replacement_lines);

    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_section_for_write_happy_path() {
        let content = "# A\nfirst\n# B\nsecond\nmore\n# C\nthird";
        let section_b_content = "# B\nsecond\nmore";
        let hash = ContentHash::from_content(section_b_content);

        let resolved = resolve_section_for_write(content, "B", hash.as_str()).unwrap();
        assert_eq!(resolved.start_line, 3);
        assert_eq!(resolved.end_line, 5);
        assert_eq!(resolved.section_content, section_b_content);
    }

    #[test]
    fn resolve_section_for_write_not_found() {
        let content = "# A\nbody";
        let err = resolve_section_for_write(content, "Nonexistent", "any_hash").unwrap_err();
        assert_eq!(
            err,
            SectionWriteError::NotFound {
                path: "Nonexistent".to_string()
            }
        );
    }

    #[test]
    fn resolve_section_for_write_ambiguous() {
        let content = "# Notes\nfirst\n# Notes\nsecond";
        let err = resolve_section_for_write(content, "Notes", "any_hash").unwrap_err();
        match err {
            SectionWriteError::Ambiguous { path, candidates } => {
                assert_eq!(path, "Notes");
                assert_eq!(candidates, vec!["Notes".to_string(), "Notes".to_string()]);
            }
            other => panic!("expected Ambiguous, got {:?}", other),
        }
    }

    #[test]
    fn resolve_section_for_write_hash_mismatch_carries_no_hash() {
        let content = "# A\nbody";
        let err = resolve_section_for_write(content, "A", "stale_hash").unwrap_err();
        assert_eq!(
            err,
            SectionWriteError::HashMismatch {
                path: "A".to_string()
            }
        );
        // The error type structurally carries no current-hash field - this
        // assertion documents that guarantee rather than testing a
        // Display/Debug string (there isn't one here to redact from).
    }

    #[test]
    fn resolve_section_for_edit_happy_path() {
        let content = "# A\nfirst\n# B\nsecond\nmore\n# C\nthird";
        let resolved = resolve_section_for_edit(content, "B").unwrap();
        assert_eq!(resolved.start_line, 3);
        assert_eq!(resolved.end_line, 5);
        assert_eq!(resolved.section_content, "# B\nsecond\nmore");
    }

    #[test]
    fn resolve_section_for_edit_not_found() {
        let content = "# A\nbody";
        let err = resolve_section_for_edit(content, "Nonexistent").unwrap_err();
        assert_eq!(
            err,
            SectionWriteError::NotFound {
                path: "Nonexistent".to_string()
            }
        );
    }

    #[test]
    fn resolve_section_for_edit_ambiguous() {
        let content = "# Notes\nfirst\n# Notes\nsecond";
        let err = resolve_section_for_edit(content, "Notes").unwrap_err();
        match err {
            SectionWriteError::Ambiguous { path, candidates } => {
                assert_eq!(path, "Notes");
                assert_eq!(candidates, vec!["Notes".to_string(), "Notes".to_string()]);
            }
            other => panic!("expected Ambiguous, got {:?}", other),
        }
    }

    #[test]
    fn resolve_section_for_edit_never_reports_hash_mismatch() {
        // Hash-free by contract: an edit scoped to a section resolves it
        // without any expected hash to compare against.
        let content = "# A\nbody";
        let resolved = resolve_section_for_edit(content, "A").unwrap();
        assert_eq!(resolved.section_content, "# A\nbody");
    }

    #[test]
    fn splice_section_no_op_reproduces_input_byte_for_byte() {
        let content = "# A\nfirst\n# B\nsecond\nmore\n# C\nthird";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();
        let extracted = extract_section(content, section_b);

        let spliced = splice_section(
            content,
            section_b.start_line,
            section_b.end_line,
            &extracted,
        );
        assert_eq!(spliced, content);
    }

    #[test]
    fn splice_section_no_op_last_section_with_trailing_newline() {
        let content = "# A\nfirst\n# B\nlast\n";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();
        let extracted = extract_section(content, section_b);

        let spliced = splice_section(
            content,
            section_b.start_line,
            section_b.end_line,
            &extracted,
        );
        assert_eq!(spliced, content);
    }

    #[test]
    fn splice_section_no_op_last_section_without_trailing_newline() {
        let content = "# A\nfirst\n# B\nlast";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();
        let extracted = extract_section(content, section_b);

        let spliced = splice_section(
            content,
            section_b.start_line,
            section_b.end_line,
            &extracted,
        );
        assert_eq!(spliced, content);
    }

    #[test]
    fn splice_section_replaces_only_target_range() {
        let content = "# A\nfirst\n# B\nsecond\nmore\n# C\nthird";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();

        let spliced = splice_section(
            content,
            section_b.start_line,
            section_b.end_line,
            "# B\nreplaced",
        );
        assert_eq!(spliced, "# A\nfirst\n# B\nreplaced\n# C\nthird");
    }
}
