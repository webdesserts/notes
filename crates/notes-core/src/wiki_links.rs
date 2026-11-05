//! Parser for Obsidian-style wiki links
//!
//! Supports:
//! - Basic links: `[[Note]]`
//! - Aliases: `[[Note|Display Text]]`
//! - Headers: `[[Note#Header]]`
//! - Block references: `[[Note#^block-id]]`
//! - Embeds: `![[Note]]`
//! - Paths: `[[folder/Note]]`

use serde::{Deserialize, Serialize};

/// A parsed wiki link from Obsidian markdown content.
///
/// Field naming follows Rust's `std::path::Path` conventions where applicable.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WikiLink {
    /// The note name without path or extension: "Note" (like `Path::file_stem()`)
    pub name: String,
    /// The parent directory path: "private/knowledge" or None for root (like `Path::parent()`)
    pub parent: Option<String>,
    /// File extension without the dot: "md" or None (like `Path::extension()`)
    pub extension: Option<String>,
    /// Header reference if present: "Header Section"
    pub header: Option<String>,
    /// Block ID if present: "block-123"
    pub block_id: Option<String>,
    /// Display alias if present: "my custom text"
    pub alias: Option<String>,
    /// Whether this is an embed (`![[...]]`)
    pub is_embed: bool,
}

impl WikiLink {
    /// Returns the file name with extension if present: "Note.md" or "Note"
    pub fn file_name(&self) -> String {
        match &self.extension {
            Some(ext) => format!("{}.{}", self.name, ext),
            None => self.name.clone(),
        }
    }

    /// Returns the full path without fragment: "private/knowledge/Note.md"
    pub fn path(&self) -> String {
        match &self.parent {
            Some(parent) => format!("{}/{}", parent, self.file_name()),
            None => self.file_name(),
        }
    }

    /// Returns alias if present, otherwise the name
    pub fn display_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// Parse all wiki links from markdown content
pub fn parse_wiki_links(content: &str) -> Vec<WikiLink> {
    let mut links = Vec::new();
    let chars: Vec<char> = content.chars().collect();
    let len = chars.len();
    let mut i = 0;

    while i < len {
        // Check for embed: ![[
        if i + 2 < len
            && chars[i] == '!'
            && chars[i + 1] == '['
            && chars[i + 2] == '['
            && let Some((link, end)) = parse_link_at(&chars, i + 1, true)
        {
            links.push(link);
            i = end;
            continue;
        }
        // Check for regular link: [[ (but not preceded by !)
        if i + 1 < len && chars[i] == '[' && chars[i + 1] == '[' {
            // Make sure it's not part of an embed we already handled
            if (i == 0 || chars[i - 1] != '!')
                && let Some((link, end)) = parse_link_at(&chars, i, false)
            {
                links.push(link);
                i = end;
                continue;
            }
        }
        i += 1;
    }

    links
}

/// Parse a link starting at position `start` (pointing to first `[`)
/// Returns the parsed link and the position after the closing `]]`
fn parse_link_at(chars: &[char], start: usize, is_embed: bool) -> Option<(WikiLink, usize)> {
    let len = chars.len();

    // Verify we have [[
    if start + 1 >= len || chars[start] != '[' || chars[start + 1] != '[' {
        return None;
    }

    // Find the closing ]]
    let content_start = start + 2;
    let mut i = content_start;
    let mut depth = 1;

    while i < len {
        if i + 1 < len && chars[i] == ']' && chars[i + 1] == ']' {
            depth -= 1;
            if depth == 0 {
                // Found closing ]]
                let content: String = chars[content_start..i].iter().collect();
                let link = parse_link_content(&content, is_embed);
                return Some((link, i + 2));
            }
        }
        if i + 1 < len && chars[i] == '[' && chars[i + 1] == '[' {
            depth += 1;
            i += 2;
            continue;
        }
        i += 1;
    }

    None
}

/// Parse the content inside [[ ]] into a WikiLink
fn parse_link_content(content: &str, is_embed: bool) -> WikiLink {
    // Split by | for alias
    let (target_part, alias) = if let Some(pipe_pos) = content.find('|') {
        let target = &content[..pipe_pos];
        let alias = &content[pipe_pos + 1..];
        (target, Some(alias.to_string()))
    } else {
        (content, None)
    };

    // Parse target for header/block references
    let (path_part, header, block_id) = parse_fragment(target_part);

    // Parse the path into parent, name, extension
    let (parent, name, extension) = parse_path(path_part);

    WikiLink {
        name,
        parent,
        extension,
        header,
        block_id,
        alias,
        is_embed,
    }
}

/// Parse a target string to extract the path and any fragment (header or block reference)
/// Returns (path_part, header, block_id)
fn parse_fragment(target: &str) -> (&str, Option<String>, Option<String>) {
    // Check for block reference: Note#^block-id
    if let Some(block_pos) = target.find("#^") {
        let path_part = &target[..block_pos];
        let block_id = &target[block_pos + 2..];
        return (path_part, None, Some(block_id.to_string()));
    }

    // Check for header reference: Note#Header
    if let Some(header_pos) = target.find('#') {
        let path_part = &target[..header_pos];
        let header = &target[header_pos + 1..];
        return (path_part, Some(header.to_string()), None);
    }

    // No fragment
    (target, None, None)
}

/// Parse a path string into parent, name, and extension
/// Returns (parent, name, extension)
fn parse_path(path: &str) -> (Option<String>, String, Option<String>) {
    let path = path.trim();

    // Split into parent and file_name
    let (parent, file_name) = if let Some(slash_pos) = path.rfind('/') {
        let parent = &path[..slash_pos];
        let file_name = &path[slash_pos + 1..];
        (Some(parent.to_string()), file_name)
    } else {
        (None, path)
    };

    // Split file_name into name and extension at the last dot
    let (name, extension) = if let Some(dot_pos) = file_name.rfind('.') {
        let name = &file_name[..dot_pos];
        let ext = &file_name[dot_pos + 1..];
        (name.to_string(), Some(ext.to_string()))
    } else {
        (file_name.to_string(), None)
    };

    (parent, name, extension)
}

/// Extract all unique note names from wiki links in content
///
/// A link's `name`/`extension` split can't distinguish a genuinely dotted note
/// name (`[[llama.cpp]]`) from an explicit `.md`-suffixed link (`[[Note.md]]`) —
/// both split at the last dot. Only the latter should have its extension
/// stripped, to match how a real file's `Path::file_stem()` only ever strips a
/// trailing `.md`; any other extension is kept as part of the note's name.
pub fn extract_linked_notes(content: &str) -> Vec<String> {
    let links = parse_wiki_links(content);
    let mut seen = std::collections::HashSet::new();
    let mut result = Vec::new();

    for link in links {
        let note_name = match link.extension.as_deref() {
            Some("md") => link.name.clone(),
            _ => link.file_name(),
        };
        if seen.insert(note_name.clone()) {
            result.push(note_name);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_wiki_links() {
        let content = "- [[CLAUDE]] - test\n- [[CLAUDE.local]] - another";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].name, "CLAUDE");
        assert_eq!(links[0].parent, None);
        assert_eq!(links[0].extension, None);
        assert!(!links[0].is_embed);
        // "CLAUDE.local" parses as name="CLAUDE", extension="local"
        assert_eq!(links[1].name, "CLAUDE");
        assert_eq!(links[1].extension, Some("local".to_string()));
        assert_eq!(links[1].file_name(), "CLAUDE.local");
    }

    #[test]
    fn parse_links_with_aliases() {
        let content = "[[Note Name|Display Text]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "Note Name");
        assert_eq!(links[0].alias, Some("Display Text".to_string()));
        assert_eq!(links[0].display_name(), "Display Text");
    }

    #[test]
    fn parse_links_with_headers() {
        let content = "[[Note#Header Section]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "Note");
        assert_eq!(links[0].header, Some("Header Section".to_string()));
    }

    #[test]
    fn parse_links_with_block_references() {
        let content = "[[Note#^block-123]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "Note");
        assert_eq!(links[0].block_id, Some("block-123".to_string()));
    }

    #[test]
    fn parse_embed_links() {
        let content = "![[Image]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "Image");
        assert!(links[0].is_embed);
    }

    #[test]
    fn parse_links_with_paths() {
        let content = "[[folder/subfolder/Note]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "Note");
        assert_eq!(links[0].parent, Some("folder/subfolder".to_string()));
        assert_eq!(links[0].path(), "folder/subfolder/Note");
    }

    #[test]
    fn handle_multiple_links_in_one_line() {
        let content = "See [[Note1]] and [[Note2]] for details";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 2);
        assert_eq!(links[0].name, "Note1");
        assert_eq!(links[1].name, "Note2");
    }

    #[test]
    fn handle_links_with_md_extension() {
        let content = "[[Note.md]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        assert_eq!(links[0].name, "Note");
        assert_eq!(links[0].extension, Some("md".to_string()));
        assert_eq!(links[0].file_name(), "Note.md");
    }

    #[test]
    fn parse_full_complex_link() {
        let content = "[[private/knowledge/Note.md#header|my note]]";
        let links = parse_wiki_links(content);

        assert_eq!(links.len(), 1);
        let link = &links[0];
        assert_eq!(link.name, "Note");
        assert_eq!(link.parent, Some("private/knowledge".to_string()));
        assert_eq!(link.extension, Some("md".to_string()));
        assert_eq!(link.header, Some("header".to_string()));
        assert_eq!(link.alias, Some("my note".to_string()));
        assert_eq!(link.file_name(), "Note.md");
        assert_eq!(link.path(), "private/knowledge/Note.md");
        assert_eq!(link.display_name(), "my note");
    }

    #[test]
    fn display_name_falls_back_to_name() {
        let content = "[[Note]]";
        let links = parse_wiki_links(content);

        assert_eq!(links[0].display_name(), "Note");
    }

    #[test]
    fn extract_unique_note_names() {
        let content = "
            - [[CLAUDE]] - test
            - [[other/CLAUDE]] - same name different path
            - [[CLAUDE]] - duplicate
        ";
        let notes = extract_linked_notes(content);

        // Dedupes by name only
        assert_eq!(notes.len(), 1);
        assert!(notes.contains(&"CLAUDE".to_string()));
    }

    #[test]
    fn extract_notes_from_complex_content() {
        let content = "
            # Knowledge Index

            ## Meta
            - [[CLAUDE]] - General vault navigation
            - [[other/WORK]] - Current work

            ## Projects
            - [[Obsidian Memory MCP Server]]
        ";
        let notes = extract_linked_notes(content);

        assert_eq!(notes.len(), 3);
        assert!(notes.contains(&"CLAUDE".to_string()));
        assert!(notes.contains(&"WORK".to_string()));
        assert!(notes.contains(&"Obsidian Memory MCP Server".to_string()));
    }

    #[test]
    fn handle_embeds_and_regular_links() {
        let content = "![[Image]] and [[Note]]";
        let notes = extract_linked_notes(content);

        assert_eq!(notes.len(), 2);
        assert!(notes.contains(&"Image".to_string()));
        assert!(notes.contains(&"Note".to_string()));
    }

    #[test]
    fn return_empty_for_no_links() {
        let content = "Just some text with no links";
        let notes = extract_linked_notes(content);

        assert!(notes.is_empty());
    }

    #[test]
    fn extract_dotted_note_name() {
        let content = "[[llama.cpp]]";
        let notes = extract_linked_notes(content);

        assert_eq!(notes, vec!["llama.cpp".to_string()]);
    }

    #[test]
    fn extract_dotted_note_name_with_decimal_version() {
        let content = "[[Qwen 3.8]]";
        let notes = extract_linked_notes(content);

        assert_eq!(notes, vec!["Qwen 3.8".to_string()]);
    }

    #[test]
    fn extract_dotted_note_name_strips_parent_path() {
        let content = "[[knowledge/llama.cpp]]";
        let notes = extract_linked_notes(content);

        assert_eq!(notes, vec!["llama.cpp".to_string()]);
    }

    #[test]
    fn extract_explicit_md_extension_still_strips_it() {
        // Guard: an explicit `.md`-suffixed link is indistinguishable from a
        // dotted name at the parser level (both split at the last dot), so the
        // extractor must special-case exactly this extension to match
        // `Path::file_stem()`'s behavior on the corresponding file.
        let content = "[[Note.md]]";
        let notes = extract_linked_notes(content);

        assert_eq!(notes, vec!["Note".to_string()]);
    }
}
