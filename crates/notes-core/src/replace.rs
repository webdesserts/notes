//! Pure exact-replacement kernel: hash-free `oldText`/`newText` batch edits
//! with whole-scope uniqueness, shared by every `replace_in_note`-style
//! adapter (the Memory MCP tool today; the native note-replacement tool is
//! the intended second consumer via a byte-identical fixture snapshot).
//!
//! This module is deliberately protocol-free: no MCP types, no storage, no
//! hashing. Adapters own note resolution, section scoping, splicing, and
//! hashing; this kernel owns only the match/uniqueness/progressive-apply
//! contract, so both adapters exhibit byte-identical replacement behavior.
//!
//! ## Pinned semantics
//!
//! - **Exact matches only.** Each `oldText` must occur as a literal substring
//!   of the content being edited (the whole note for an unscoped replace, the
//!   resolved section for a scoped one). No normalization on either side.
//! - **One possible match.** Uniqueness counts *overlapping* occurrences, not
//!   just non-overlapping ones: `aa` in `aaa` is ambiguous even though
//!   [`str::matches`] would report a single non-overlapping match. Every
//!   starting position where `oldText` fits counts as a possible match, and
//!   more than one possible match is an ambiguity error.
//! - **Progressive batch, all-validated-before-save.** Edits apply in order,
//!   each seeing the output of the previous one (edit N+1 may target text
//!   created by edit N). The kernel validates and applies the whole batch in
//!   memory and returns the combined result; adapters must not write anything
//!   unless the *entire* batch validated, so a failure on edit N (even after
//!   edits 1..N-1 already succeeded in memory) leaves the note's bytes
//!   untouched. There are no partial writes.
//! - **Empty `oldText` behavior is preserved, not widened.** The empty string
//!   "matches" at every character boundary, so against any non-empty content
//!   it is ambiguous; against empty content it counts as one match and
//!   replaces the (empty) whole. This mirrors the historical
//!   `contains`/`matches`-count behavior this kernel replaced.

/// A single exact-replacement operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplacementEdit {
    /// Text to search for - must identify exactly one possible match in the
    /// content being edited (overlapping occurrences all count).
    pub old_text: String,
    /// Text to replace the matched occurrence with.
    pub new_text: String,
}

/// Why an edit was refused. Carries the offending text (never the content
/// around it) so adapters can build teaching errors without the kernel
/// knowing any display policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplacementError {
    /// The edit's `oldText` does not occur in the content being edited.
    NotFound { old_text: String },
    /// The edit's `oldText` occurs more than once (counting overlapping
    /// occurrences) in the content being edited.
    Ambiguous { old_text: String, count: usize },
}

/// The edit that failed and why. The batch is refused as a whole; earlier
/// edits' in-memory output is discarded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedEdit {
    /// Zero-based position of the failing edit in the submitted batch.
    pub index: usize,
    /// Why that edit failed.
    pub error: ReplacementError,
}

/// Count every starting position in `haystack` where `needle` occurs,
/// including overlapping ones (`aa` occurs twice in `aaa`).
///
/// The empty needle counts once per character boundary (plus the end
/// position), mirroring [`str::matches`]' empty-pattern behavior.
pub fn count_overlapping_occurrences(haystack: &str, needle: &str) -> usize {
    if needle.is_empty() {
        return haystack.char_indices().count() + 1;
    }
    let hay = haystack.as_bytes();
    let nee = needle.as_bytes();
    if nee.len() > hay.len() {
        return 0;
    }
    let mut count = 0;
    for start in 0..=hay.len() - nee.len() {
        if &hay[start..start + nee.len()] == nee {
            count += 1;
        }
    }
    count
}

/// Apply a batch of exact replacements progressively to `content`.
///
/// Returns the combined modified content only if *every* edit validated;
/// otherwise returns the [`RejectedEdit`] describing the first failure, and
/// the caller must leave the underlying note byte-for-byte unchanged.
///
/// Each edit's uniqueness check runs against the in-memory output of all
/// previous edits - see the module docs for the full pinned contract.
pub fn apply_replacements(
    content: &str,
    edits: &[ReplacementEdit],
) -> Result<String, RejectedEdit> {
    let mut modified = content.to_string();

    for (index, edit) in edits.iter().enumerate() {
        if !modified.contains(&edit.old_text) {
            return Err(RejectedEdit {
                index,
                error: ReplacementError::NotFound {
                    old_text: edit.old_text.clone(),
                },
            });
        }

        let count = count_overlapping_occurrences(&modified, &edit.old_text);
        if count > 1 {
            return Err(RejectedEdit {
                index,
                error: ReplacementError::Ambiguous {
                    old_text: edit.old_text.clone(),
                    count,
                },
            });
        }

        modified = modified.replacen(&edit.old_text, &edit.new_text, 1);
    }

    Ok(modified)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn edit(old: &str, new: &str) -> ReplacementEdit {
        ReplacementEdit {
            old_text: old.to_string(),
            new_text: new.to_string(),
        }
    }

    #[test]
    fn single_occurrence_applies() {
        let out = apply_replacements("Hello, world!", &[edit("world", "Rust")]).unwrap();
        assert_eq!(out, "Hello, Rust!");
    }

    #[test]
    fn zero_occurrences_not_found() {
        let err = apply_replacements("alpha", &[edit("gamma", "x")]).unwrap_err();
        assert_eq!(
            err,
            RejectedEdit {
                index: 0,
                error: ReplacementError::NotFound {
                    old_text: "gamma".to_string()
                }
            }
        );
    }

    #[test]
    fn two_occurrences_ambiguous() {
        let err = apply_replacements("todo one\ntodo two", &[edit("todo", "done")]).unwrap_err();
        assert_eq!(
            err,
            RejectedEdit {
                index: 0,
                error: ReplacementError::Ambiguous {
                    old_text: "todo".to_string(),
                    count: 2
                }
            }
        );
    }

    #[test]
    fn overlapping_occurrences_are_ambiguous() {
        // `aa` fits at two positions in `aaa` even though a non-overlapping
        // match count would report one - both count as possible matches.
        assert_eq!(count_overlapping_occurrences("aaa", "aa"), 2);
        let err = apply_replacements("aaa", &[edit("aa", "b")]).unwrap_err();
        assert_eq!(
            err,
            RejectedEdit {
                index: 0,
                error: ReplacementError::Ambiguous {
                    old_text: "aa".to_string(),
                    count: 2
                }
            }
        );
    }

    #[test]
    fn overlapping_occurrences_in_repetition_count_individually() {
        let err = apply_replacements("aaaa", &[edit("aa", "b")]).unwrap_err();
        assert!(matches!(
            err.error,
            ReplacementError::Ambiguous { count: 3, .. }
        ));
    }

    #[test]
    fn later_edit_sees_earlier_output() {
        let out =
            apply_replacements("alpha", &[edit("alpha", "beta"), edit("beta", "gamma")]).unwrap();
        assert_eq!(out, "gamma");
    }

    #[test]
    fn failure_after_successful_in_memory_edit_reports_failing_index() {
        let err = apply_replacements(
            "keep old chunk",
            &[
                edit("old chunk", "new chunk"),
                edit("absent text", "never applied"),
            ],
        )
        .unwrap_err();
        assert_eq!(err.index, 1);
        assert!(matches!(err.error, ReplacementError::NotFound { .. }));
    }

    #[test]
    fn empty_batch_returns_content_unchanged() {
        assert_eq!(apply_replacements("same", &[]).unwrap(), "same");
    }

    #[test]
    fn empty_oldtext_is_ambiguous_against_nonempty_content() {
        // Preserved historical behavior: "" "matches" at every char boundary.
        let err = apply_replacements("hello", &[edit("", "x")]).unwrap_err();
        assert!(matches!(
            err.error,
            ReplacementError::Ambiguous { count: 6, .. }
        ));
    }

    #[test]
    fn empty_oldtext_against_empty_content_is_single_match() {
        // Preserved historical behavior: replacen("", "x", 1) on "" inserts.
        let out = apply_replacements("", &[edit("", "x")]).unwrap();
        assert_eq!(out, "x");
    }

    #[test]
    fn multibyte_boundaries_are_respected() {
        // A needle that only byte-aligns inside a multibyte char must not
        // count as a match: the byte scan can only report positions whose
        // window equals the needle's own valid UTF-8 bytes.
        assert_eq!(count_overlapping_occurrences("héllo héllo", "é"), 2);
        assert_eq!(count_overlapping_occurrences("héllo", "\u{c3}"), 0);
    }
}
