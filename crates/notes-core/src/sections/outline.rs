//! Section model: builds a flat outline of a note's addressable sections
//! (frontmatter, preamble, and heading-delimited sections) and extracts a
//! single section's raw content by its line range.

use crate::frontmatter::split_frontmatter;

use super::heading;
use super::heading::parse_headings;

/// What kind of section this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionKind {
    /// The YAML frontmatter block (present iff a delimited `---` block exists,
    /// regardless of whether the YAML inside is valid).
    Frontmatter,
    /// Content between frontmatter (or file start) and the first heading.
    Preamble,
    /// A heading-delimited section.
    Heading,
}

/// A single addressable section of a note.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Section {
    /// What kind of section this is.
    pub kind: SectionKind,
    /// Heading level (1-6 for headings; 0 for the Frontmatter/Preamble pseudo-sections).
    pub level: u8,
    /// Heading text ("Frontmatter"/"Preamble" for pseudo-sections).
    pub text: String,
    /// Full ancestor chain of heading texts, joined by `" > "`. Single-segment
    /// for pseudo-sections and top-level headings. Display-only - path
    /// resolution matches against `segments`, not by re-splitting this string,
    /// so a heading whose own text happens to contain the literal `" > "`
    /// separator doesn't get silently mis-parsed into extra segments.
    pub path: String,
    /// The ancestor chain as untouched heading texts (outermost to self), one
    /// entry per real heading level - never re-split, even if a heading's
    /// text contains the `" > "` separator. This is the structural segment
    /// list `path` resolution matches against.
    pub segments: Vec<String>,
    /// First line of the section, 1-indexed, inclusive.
    pub start_line: usize,
    /// Last line of the section, 1-indexed, inclusive.
    pub end_line: usize,
    /// Character count of the section's extracted content.
    pub size_chars: usize,
}

/// The full outline of a note: a flat list of its addressable sections in
/// document order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outline {
    pub sections: Vec<Section>,
}

/// Build the outline for `content`.
///
/// Sections are returned in document order: Frontmatter (if present), Preamble
/// (if non-empty), then each heading section. See the [`crate::sections`]
/// module doc for the full addressing grammar.
pub fn build_outline(content: &str) -> Outline {
    let mut sections = Vec::new();

    if content.is_empty() {
        return Outline { sections };
    }

    let lines: Vec<&str> = content.split('\n').collect();
    let total_lines = lines.len();

    let (frontmatter_yaml, body) = split_frontmatter(content);
    let mut body_start_line = 1;

    if let Some(yaml) = frontmatter_yaml {
        // The frontmatter block spans line 1 through the closing "---" line,
        // inclusive. The closing delimiter line is the line right after the
        // yaml block's own line count (yaml is everything between the
        // delimiters, so its line count plus the opening "---" line gives us
        // the closing delimiter's line number).
        let yaml_line_count = yaml.split('\n').count();
        let closing_line = 1 + yaml_line_count;
        sections.push(Section {
            kind: SectionKind::Frontmatter,
            level: 0,
            text: "Frontmatter".to_string(),
            path: "Frontmatter".to_string(),
            segments: vec!["Frontmatter".to_string()],
            start_line: 1,
            end_line: closing_line,
            size_chars: 0, // filled in below once we know the full extracted range
        });
        body_start_line = closing_line + 1;
    }

    // Heading detection never scans the frontmatter-delimited region: an
    // ordinary YAML full-line comment (e.g. "# a note about this field") is
    // syntactically identical to an ATX heading and would otherwise leak a
    // bogus heading into the outline. Parse only the post-frontmatter body,
    // then offset each heading's line number back to be absolute within the
    // whole file.
    let heading_scan_target = if frontmatter_yaml.is_some() {
        body
    } else {
        content
    };
    let line_offset = body_start_line - 1;
    let headings: Vec<heading::HeadingLine> = parse_headings(heading_scan_target)
        .into_iter()
        .map(|h| heading::HeadingLine {
            line: h.line + line_offset,
            ..h
        })
        .collect();
    let first_heading_line = headings.first().map(|h| h.line);

    let preamble_end_line = first_heading_line.map(|l| l - 1).unwrap_or(total_lines);
    // A candidate span that reduces to exactly the phantom empty final line
    // produced by a trailing newline (see the extraction contract) has no
    // real content - e.g. frontmatter immediately followed by nothing but a
    // trailing newline. Treat that as an empty preamble, not a one-line one.
    let is_only_phantom_trailing_line = body_start_line == preamble_end_line
        && preamble_end_line == total_lines
        && lines[preamble_end_line - 1].is_empty();
    if body_start_line <= preamble_end_line && !is_only_phantom_trailing_line {
        sections.push(Section {
            kind: SectionKind::Preamble,
            level: 0,
            text: "Preamble".to_string(),
            path: "Preamble".to_string(),
            segments: vec!["Preamble".to_string()],
            start_line: body_start_line,
            end_line: preamble_end_line,
            size_chars: 0,
        });
    }

    // Ancestor stack: (level, text) pairs for building each heading's full path.
    let mut ancestors: Vec<(u8, String)> = Vec::new();

    for (idx, heading) in headings.iter().enumerate() {
        // Pop ancestors at the same or deeper level than this heading.
        while ancestors
            .last()
            .is_some_and(|(level, _)| *level >= heading.level)
        {
            ancestors.pop();
        }

        let mut segments: Vec<String> = ancestors.iter().map(|(_, text)| text.clone()).collect();
        segments.push(heading.text.clone());
        // Display path only - joining is one-way. Resolution matches against
        // `segments` directly rather than re-splitting this string, so a
        // heading whose own text embeds " > " doesn't get parsed into extra
        // segments it doesn't structurally have.
        let path = segments.join(" > ");

        // A section ends the line before the next heading of same-or-higher
        // level; find that boundary by scanning forward.
        let end_line = headings[idx + 1..]
            .iter()
            .find(|next| next.level <= heading.level)
            .map(|next| next.line - 1)
            .unwrap_or(total_lines);

        sections.push(Section {
            kind: SectionKind::Heading,
            level: heading.level,
            text: heading.text.clone(),
            path,
            segments,
            start_line: heading.line,
            end_line,
            size_chars: 0,
        });

        ancestors.push((heading.level, heading.text.clone()));
    }

    // Fill in size_chars now that every section's line range is final.
    for section in &mut sections {
        section.size_chars = extract_section(content, section).chars().count();
    }

    Outline { sections }
}

/// Extract a section's raw content from `content`.
///
/// This is the single extraction path shared by reads and writes: returns
/// `content.split('\n')[start_line..=end_line]` (1-indexed) joined with `"\n"`,
/// with nothing prepended or appended. A no-op splice of this string back into
/// the same range must reproduce the input byte-for-byte.
pub fn extract_section(content: &str, section: &Section) -> String {
    let lines: Vec<&str> = content.split('\n').collect();
    let start = section.start_line - 1;
    let end = section.end_line; // exclusive upper bound for slicing
    lines[start..end].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::ContentHash;

    fn paths(outline: &Outline) -> Vec<&str> {
        outline.sections.iter().map(|s| s.path.as_str()).collect()
    }

    #[test]
    fn empty_content_has_zero_sections() {
        // 24 genuinely 0-byte files exist in the live vault.
        let outline = build_outline("");
        assert_eq!(outline.sections.len(), 0);
    }

    #[test]
    fn multi_level_nested_document_has_correct_paths_and_ranges() {
        // Two top-level sections so a section's boundary against a
        // same-or-higher-level heading is actually exercised: "Top"'s only
        // same-or-higher-level successor is "Top Two", so nested children
        // ("Middle"/"Leaf") do NOT end it early.
        let content = "\
# Top
intro text
## Middle
middle text
### Leaf
leaf text
# Top Two
more text";
        let outline = build_outline(content);

        assert_eq!(
            paths(&outline),
            vec!["Top", "Top > Middle", "Top > Middle > Leaf", "Top Two"]
        );

        let top = &outline.sections[0];
        assert_eq!(top.level, 1);
        assert_eq!(top.start_line, 1);
        assert_eq!(top.end_line, 6); // ends right before "# Top Two" (same level)

        let middle = &outline.sections[1];
        assert_eq!(middle.level, 2);
        assert_eq!(middle.start_line, 3);
        assert_eq!(middle.end_line, 6); // "### Leaf" is a child, not a boundary; ends before "# Top Two"

        let leaf = &outline.sections[2];
        assert_eq!(leaf.level, 3);
        assert_eq!(leaf.start_line, 5);
        assert_eq!(leaf.end_line, 6); // ends right before "# Top Two" (higher level)

        let top_two = &outline.sections[3];
        assert_eq!(top_two.level, 1);
        assert_eq!(top_two.start_line, 7);
        assert_eq!(top_two.end_line, 8); // last section, ends at EOF
    }

    #[test]
    fn size_chars_counts_unicode_chars_not_bytes() {
        // "🎉" is 4 bytes in UTF-8 but 1 char.
        let content = "# Heading\n🎉🎉🎉";
        let outline = build_outline(content);
        let heading = &outline.sections[0];
        // "# Heading\n🎉🎉🎉" extracted -> "# Heading\n🎉🎉🎉" (10 + 1 + 3 = 14 chars)
        assert_eq!(heading.size_chars, "# Heading\n🎉🎉🎉".chars().count());
        assert_ne!(
            heading.size_chars,
            "# Heading\n🎉🎉🎉".len(),
            "size_chars should count chars, not bytes"
        );
    }

    #[test]
    fn frontmatter_pseudo_section_present_with_valid_yaml() {
        let content = "---\ntitle: Test\n---\nBody content";
        let outline = build_outline(content);
        let frontmatter = outline
            .sections
            .iter()
            .find(|s| s.kind == SectionKind::Frontmatter)
            .expect("frontmatter section should be present");
        assert_eq!(frontmatter.start_line, 1);
        assert_eq!(frontmatter.end_line, 3); // closing "---" is line 3
        assert_eq!(frontmatter.path, "Frontmatter");
        assert_eq!(frontmatter.level, 0);
    }

    #[test]
    fn frontmatter_pseudo_section_present_with_invalid_yaml_but_delimited() {
        // split_frontmatter finds the delimited block regardless of YAML validity.
        let content = "---\nnot: valid: yaml: at: all\n---\nBody content";
        let outline = build_outline(content);
        let frontmatter = outline
            .sections
            .iter()
            .find(|s| s.kind == SectionKind::Frontmatter)
            .expect("frontmatter section should be present even with invalid YAML");
        assert_eq!(frontmatter.end_line, 3);
    }

    #[test]
    fn frontmatter_containing_a_bare_comment_line_produces_zero_headings_from_that_region() {
        // A completely ordinary YAML full-line comment is syntactically
        // identical to an ATX heading. Heading detection must never scan
        // inside the frontmatter span, or this would inject a bogus heading.
        let content = "---\n# a note about this field\ntitle: Test\n---\n# Real Heading\nbody";
        let outline = build_outline(content);

        let headings: Vec<&Section> = outline
            .sections
            .iter()
            .filter(|s| s.kind == SectionKind::Heading)
            .collect();
        assert_eq!(headings.len(), 1);
        assert_eq!(headings[0].text, "Real Heading");
    }

    #[test]
    fn frontmatter_only_note_has_no_preamble_or_headings() {
        let content = "---\ntitle: Test\n---\n";
        let outline = build_outline(content);
        assert_eq!(paths(&outline), vec!["Frontmatter"]);
    }

    #[test]
    fn preamble_absent_when_heading_immediately_follows_frontmatter() {
        let content = "---\ntitle: Test\n---\n# Heading\nbody";
        let outline = build_outline(content);
        assert_eq!(paths(&outline), vec!["Frontmatter", "Heading"]);
    }

    #[test]
    fn headingless_note_with_no_frontmatter_is_single_preamble_section() {
        let content = "Just some text.\nMore text.";
        let outline = build_outline(content);
        assert_eq!(outline.sections.len(), 1);
        assert_eq!(outline.sections[0].kind, SectionKind::Preamble);
        assert_eq!(outline.sections[0].path, "Preamble");
        assert_eq!(outline.sections[0].start_line, 1);
        assert_eq!(outline.sections[0].end_line, 2);
    }

    #[test]
    fn duplicate_sibling_heading_text_produces_distinct_sections_with_same_path() {
        let content = "# Notes\nfirst\n# Notes\nsecond";
        let outline = build_outline(content);
        assert_eq!(outline.sections.len(), 2);
        assert_eq!(outline.sections[0].path, "Notes");
        assert_eq!(outline.sections[1].path, "Notes");
        assert_ne!(
            outline.sections[0].start_line,
            outline.sections[1].start_line
        );
    }

    #[test]
    fn extract_section_middle_section() {
        let content = "# A\nfirst\n# B\nsecond\nmore\n# C\nthird";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();
        let extracted = extract_section(content, section_b);
        assert_eq!(extracted, "# B\nsecond\nmore");
    }

    #[test]
    fn extract_section_last_section_with_trailing_newline() {
        let content = "# A\nfirst\n# B\nlast\n";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();
        let extracted = extract_section(content, section_b);
        // The trailing newline produces a phantom empty final line, which the
        // last section's range includes - preserving it through extraction.
        assert_eq!(extracted, "# B\nlast\n");
        assert_eq!(
            ContentHash::from_content(&extracted),
            ContentHash::from_content("# B\nlast\n")
        );
    }

    #[test]
    fn extract_section_last_section_without_trailing_newline() {
        let content = "# A\nfirst\n# B\nlast";
        let outline = build_outline(content);
        let section_b = outline.sections.iter().find(|s| s.text == "B").unwrap();
        let extracted = extract_section(content, section_b);
        assert_eq!(extracted, "# B\nlast");
    }

    #[test]
    fn extract_section_empty_section() {
        // Heading immediately followed by a same-or-higher-level heading.
        let content = "# A\n# B\nbody";
        let outline = build_outline(content);
        let section_a = outline.sections.iter().find(|s| s.text == "A").unwrap();
        let extracted = extract_section(content, section_a);
        assert_eq!(extracted, "# A");
    }

    #[test]
    fn extract_section_frontmatter() {
        let content = "---\ntitle: Test\n---\nBody";
        let outline = build_outline(content);
        let frontmatter = outline
            .sections
            .iter()
            .find(|s| s.kind == SectionKind::Frontmatter)
            .unwrap();
        let extracted = extract_section(content, frontmatter);
        assert_eq!(extracted, "---\ntitle: Test\n---");
    }
}
