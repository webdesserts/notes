//! Shared log parsing and rendering for Log.md.
//!
//! Log.md uses a simple markdown format: day sections (`## YYYY-Www-D (Day)`)
//! followed by timestamped bullet entries (`- h:MM AM – content`).
//!
//! P2P sync can create duplicate day sections when two devices add entries
//! independently. Both the Log and WriteLogs tools use this module to parse,
//! merge duplicates, and render back to a clean file.

use std::collections::HashMap;

/// A parsed day section from the log.
#[derive(Debug, Clone)]
pub struct LogSection {
    /// The full header line, e.g. `## 2026-W09-1 (Mon)`
    pub header: String,
    /// Timestamped bullet entries belonging to this section
    pub entries: Vec<String>,
}

/// Parse Log.md content into a preamble and a list of merged day sections.
///
/// Sections with identical headers are merged, and entries within each section
/// are sorted chronologically. The section order follows first-occurrence order
/// in the original file.
pub fn parse_log_sections(content: &str) -> (Vec<String>, Vec<LogSection>) {
    let mut preamble: Vec<String> = Vec::new();
    let mut section_order: Vec<String> = Vec::new();
    let mut sections: HashMap<String, Vec<String>> = HashMap::new();
    let mut current_header: Option<String> = None;

    for line in content.lines() {
        if line.starts_with("## ") {
            let header = line.to_string();
            current_header = Some(header.clone());
            if let std::collections::hash_map::Entry::Vacant(entry) = sections.entry(header.clone())
            {
                section_order.push(header);
                entry.insert(Vec::new());
            }
        } else if let Some(ref header) = current_header {
            let trimmed = line.trim();
            if trimmed.starts_with('-') {
                sections.get_mut(header).unwrap().push(line.to_string());
            }
            // Skip blank lines — we regenerate spacing on render
        } else {
            // Lines before the first section header are preamble
            preamble.push(line.to_string());
        }
    }

    // Sort entries within each section chronologically
    let merged: Vec<LogSection> = section_order
        .into_iter()
        .map(|header| {
            let mut entries = sections.remove(&header).unwrap_or_default();
            entries.sort_by(|a, b| {
                let ta = parse_entry_time_24h(a).unwrap_or((24, 0));
                let tb = parse_entry_time_24h(b).unwrap_or((24, 0));
                ta.cmp(&tb)
            });
            LogSection { header, entries }
        })
        .collect();

    (preamble, merged)
}

/// Render preamble and sections back into Log.md content.
pub fn render_log_sections(preamble: &[String], sections: &[LogSection]) -> String {
    let mut lines: Vec<String> = Vec::new();

    // Trim trailing blank lines from preamble
    let preamble_trimmed: Vec<&String> = {
        let mut p: Vec<&String> = preamble.iter().collect();
        while p.last().map(|l| l.trim().is_empty()).unwrap_or(false) {
            p.pop();
        }
        p
    };

    for line in &preamble_trimmed {
        lines.push((*line).clone());
    }

    for section in sections {
        // Blank line before each section header
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(section.header.clone());
        lines.push(String::new());
        for entry in &section.entries {
            lines.push(entry.clone());
        }
    }

    // Trailing newline
    if !lines.is_empty() {
        lines.push(String::new());
    }

    lines.join("\n")
}

/// Parse a log entry bullet into 24-hour (hour, minute).
///
/// Expects format: `- h:MM AM – content`
pub fn parse_entry_time_24h(entry: &str) -> Option<(u32, u32)> {
    let entry = entry.trim().strip_prefix("- ")?;
    let time_end = entry.find(" – ")?;
    let time_str = &entry[..time_end];

    let parts: Vec<&str> = time_str.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }

    let time_parts: Vec<&str> = parts[0].split(':').collect();
    if time_parts.len() != 2 {
        return None;
    }

    let hour: u32 = time_parts[0].parse().ok()?;
    let minute: u32 = time_parts[1].parse().ok()?;
    let am_pm = parts[1];

    let hour_24 = match am_pm {
        "AM" if hour == 12 => 0,
        "AM" => hour,
        "PM" if hour == 12 => 12,
        "PM" => hour + 12,
        _ => return None,
    };

    Some((hour_24, minute))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_and_render_simple() {
        let content = "\
## 2026-W09-1 (Mon)

- 9:00 AM – Morning task
- 2:00 PM – Afternoon task
";

        let (preamble, sections) = parse_log_sections(content);
        assert!(preamble.is_empty());
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].entries.len(), 2);

        let rendered = render_log_sections(&preamble, &sections);
        assert_eq!(rendered, content);
    }

    #[test]
    fn test_merge_duplicate_sections() {
        let content = "\
## 2026-W09-1 (Mon)

- 9:00 AM – Morning task

## 2026-W09-1 (Mon)

- 2:00 PM – Afternoon task
";

        let (preamble, sections) = parse_log_sections(content);
        assert_eq!(sections.len(), 1, "duplicates should be merged");
        assert_eq!(sections[0].entries.len(), 2);
        assert!(sections[0].entries[0].contains("9:00 AM"));
        assert!(sections[0].entries[1].contains("2:00 PM"));

        let rendered = render_log_sections(&preamble, &sections);
        // Should only have one header
        assert_eq!(rendered.matches("## 2026-W09-1 (Mon)").count(), 1);
    }

    #[test]
    fn test_entries_sorted_chronologically_after_merge() {
        let content = "\
## 2026-W09-1 (Mon)

- 2:00 PM – Late entry

## 2026-W09-1 (Mon)

- 9:00 AM – Early entry
";

        let (_preamble, sections) = parse_log_sections(content);
        assert_eq!(sections[0].entries[0], "- 9:00 AM – Early entry");
        assert_eq!(sections[0].entries[1], "- 2:00 PM – Late entry");
    }

    #[test]
    fn test_preserves_preamble() {
        let content = "\
# Log

Some preamble text.

## 2026-W09-1 (Mon)

- 9:00 AM – Entry
";

        let (preamble, sections) = parse_log_sections(content);
        // "# Log", "", "Some preamble text.", ""
        assert_eq!(preamble.len(), 4);
        assert_eq!(sections.len(), 1);

        let rendered = render_log_sections(&preamble, &sections);
        assert!(rendered.starts_with("# Log\n\nSome preamble text."));
    }

    #[test]
    fn test_preserves_section_order() {
        let content = "\
## 2026-W09-1 (Mon)

- 9:00 AM – Monday

## 2026-W09-2 (Tue)

- 10:00 AM – Tuesday
";

        let (_preamble, sections) = parse_log_sections(content);
        assert_eq!(sections.len(), 2);
        assert!(sections[0].header.contains("W09-1"));
        assert!(sections[1].header.contains("W09-2"));
    }

    #[test]
    fn test_merge_three_duplicates() {
        let content = "\
## 2026-W09-1 (Mon)

- 9:00 AM – First

## 2026-W09-1 (Mon)

- 2:00 PM – Second

## 2026-W09-1 (Mon)

- 12:00 PM – Third
";

        let (_preamble, sections) = parse_log_sections(content);
        assert_eq!(sections.len(), 1);
        assert_eq!(sections[0].entries.len(), 3);
        // Should be sorted: 9 AM, 12 PM, 2 PM
        assert!(sections[0].entries[0].contains("9:00 AM"));
        assert!(sections[0].entries[1].contains("12:00 PM"));
        assert!(sections[0].entries[2].contains("2:00 PM"));
    }

    #[test]
    fn test_empty_content() {
        let (preamble, sections) = parse_log_sections("");
        assert!(preamble.is_empty());
        assert!(sections.is_empty());

        let rendered = render_log_sections(&preamble, &sections);
        assert_eq!(rendered, "");
    }

    #[test]
    fn test_merge_duplicates_preserves_other_sections() {
        let content = "\
## 2026-W09-1 (Mon)

- 9:00 AM – Monday morning

## 2026-W09-2 (Tue)

- 10:00 AM – Tuesday

## 2026-W09-1 (Mon)

- 2:00 PM – Monday afternoon
";

        let (_preamble, sections) = parse_log_sections(content);
        assert_eq!(sections.len(), 2);
        // Monday should be first (first-occurrence order) and merged
        assert!(sections[0].header.contains("W09-1"));
        assert_eq!(sections[0].entries.len(), 2);
        // Tuesday unchanged
        assert!(sections[1].header.contains("W09-2"));
        assert_eq!(sections[1].entries.len(), 1);
    }
}
