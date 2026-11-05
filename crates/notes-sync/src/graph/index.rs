use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use tokio::fs;
use wiki_links::extract_linked_notes;

/// Tracks forward links and backlinks between notes in the vault.
///
/// The graph index maintains a bidirectional view of wiki-link connections:
/// - Forward links: which notes does a given note link TO
/// - Backlinks: which notes link TO a given note
///
/// Notes are keyed by their relative path (e.g., "knowledge/Index.md") to avoid
/// collisions between same-named notes in different folders. Wiki-links reference
/// note names, so we maintain a name → paths lookup for resolution.
#[derive(Debug, Default)]
pub struct GraphIndex {
    /// Map from relative path to its forward links (note names from wiki-links)
    forward_links: HashMap<String, HashSet<String>>,
    /// Map from note name to paths that link TO it
    backlinks: HashMap<String, HashSet<String>>,
    /// Map from note name to all paths with that name (for wiki-link resolution)
    name_to_paths: HashMap<String, HashSet<String>>,
}

impl GraphIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize the graph index by scanning the vault.
    ///
    /// Recursively scans all markdown files in the vault, extracts wiki-links,
    /// and builds the forward links and backlinks graph.
    pub async fn initialize(&mut self, vault_path: &Path) -> Result<(), std::io::Error> {
        tracing::info!("Scanning vault for notes...");

        let files = Self::get_all_markdown_files(vault_path).await?;
        tracing::info!("Found {} markdown files", files.len());

        for file_path in files {
            if let Err(e) = self.index_file(vault_path, &file_path).await {
                tracing::warn!("Failed to index {}: {}", file_path.display(), e);
            }
        }

        tracing::info!(
            "Indexed {} notes with {} total links",
            self.len(),
            self.get_total_links()
        );

        Ok(())
    }

    /// Recursively get all markdown files in a directory.
    async fn get_all_markdown_files(dir: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
        let mut files = Vec::new();
        let mut entries = fs::read_dir(dir).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            let file_name = entry.file_name();
            let file_name_str = file_name.to_string_lossy();

            // Skip hidden directories (.obsidian, .git, .trash, etc.)
            if file_name_str.starts_with('.') {
                continue;
            }

            let file_type = entry.file_type().await?;
            if file_type.is_dir() {
                // Recursively scan subdirectories
                let sub_files = Box::pin(Self::get_all_markdown_files(&path)).await?;
                files.extend(sub_files);
            } else if file_type.is_file() && file_name_str.ends_with(".md") {
                files.push(path);
            }
        }

        Ok(files)
    }

    /// Index a single file (extract links and update graph).
    async fn index_file(
        &mut self,
        vault_path: &Path,
        file_path: &Path,
    ) -> Result<(), std::io::Error> {
        let content = fs::read_to_string(file_path).await?;

        // Get note name (filename without .md extension)
        let note_name = file_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or_default()
            .to_string();

        // Get relative path from vault root
        let relative_path = file_path
            .strip_prefix(vault_path)
            .unwrap_or(file_path)
            .to_path_buf();

        // Extract linked notes using wiki-links crate
        let linked_notes = extract_linked_notes(&content);
        let links: HashSet<String> = linked_notes.into_iter().collect();

        // Update the graph
        self.update_note(&note_name, relative_path, links);

        Ok(())
    }

    /// Get total number of links in the graph.
    fn get_total_links(&self) -> usize {
        self.forward_links.values().map(|links| links.len()).sum()
    }

    /// Add or update a note's links in the index.
    ///
    /// This removes any existing links for the note and replaces them with the new set.
    /// The path should be relative to the vault root (e.g., "knowledge/Note.md").
    pub fn update_note(&mut self, note_name: &str, path: PathBuf, links: HashSet<String>) {
        let path_key = path.to_string_lossy().to_string();

        // Remove old backlinks for this path
        if let Some(old_links) = self.forward_links.get(&path_key) {
            for target in old_links.iter() {
                if let Some(backlink_set) = self.backlinks.get_mut(target) {
                    backlink_set.remove(&path_key);
                }
            }
        }

        // Add new backlinks (target note name -> this path)
        for target in &links {
            self.backlinks
                .entry(target.clone())
                .or_default()
                .insert(path_key.clone());
        }

        // Update forward links
        self.forward_links.insert(path_key.clone(), links);

        // Update name to paths mapping
        self.name_to_paths
            .entry(note_name.to_string())
            .or_default()
            .insert(path_key);
    }

    /// Remove a note from the index entirely.
    ///
    /// The path should be the relative path used when the note was added.
    pub fn remove_note(&mut self, note_name: &str, path: &Path) {
        let path_key = path.to_string_lossy().to_string();

        // Remove forward links and their backlink entries
        if let Some(links) = self.forward_links.remove(&path_key) {
            for target in links {
                if let Some(backlink_set) = self.backlinks.get_mut(&target) {
                    backlink_set.remove(&path_key);
                }
            }
        }

        // Remove from all backlink sets (in case other notes link to this one by name)
        for backlink_set in self.backlinks.values_mut() {
            backlink_set.remove(&path_key);
        }

        // Remove from name_to_paths mapping
        if let Some(paths) = self.name_to_paths.get_mut(note_name) {
            paths.remove(&path_key);
            if paths.is_empty() {
                self.name_to_paths.remove(note_name);
            }
        }
    }

    /// Get forward links for a note by path (notes this note links TO).
    /// Returns note names (not paths) since wiki-links reference names.
    pub fn get_forward_links(&self, path: &str) -> Option<&HashSet<String>> {
        self.forward_links.get(path)
    }

    /// Get backlinks for a note name (paths that link TO this note).
    /// Input is a note name (from wiki-link), output is paths.
    pub fn get_backlinks(&self, note_name: &str) -> Option<&HashSet<String>> {
        self.backlinks.get(note_name)
    }

    /// Get all paths for a given note name.
    /// Returns None if no notes with that name exist.
    /// Returns multiple paths if there are same-named notes in different folders.
    pub fn get_paths_for_name(&self, note_name: &str) -> Option<&HashSet<String>> {
        self.name_to_paths.get(note_name)
    }

    /// Get the first path for a note name (for backward compatibility).
    /// Prefer get_paths_for_name when handling potential duplicates.
    pub fn get_path(&self, note_name: &str) -> Option<PathBuf> {
        self.name_to_paths
            .get(note_name)
            .and_then(|paths| paths.iter().next())
            .map(PathBuf::from)
    }

    /// Get all paths in the index.
    pub fn all_paths(&self) -> impl Iterator<Item = &String> {
        self.forward_links.keys()
    }

    /// Get the total number of notes in the index.
    pub fn len(&self) -> usize {
        self.forward_links.len()
    }

    /// Check if the index is empty.
    // kept: companion to `len()` (clippy expects the pair) and used by the graph tests.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.forward_links.is_empty()
    }

    /// Get all connected paths for a note path - its "neighborhood".
    /// Returns paths (not note names) for graph traversal algorithms.
    pub fn get_neighborhood(&self, path: &str) -> HashSet<String> {
        let mut neighborhood = HashSet::new();

        // Forward links from this path are note names - resolve to paths
        if let Some(forward_names) = self.get_forward_links(path) {
            for name in forward_names {
                if let Some(paths) = self.get_paths_for_name(name) {
                    neighborhood.extend(paths.iter().cloned());
                }
            }
        }

        // Find this note's name from path, then get backlinks (which are paths)
        if let Some(note_name) = Path::new(path).file_stem().and_then(|s| s.to_str())
            && let Some(back_paths) = self.get_backlinks(note_name)
        {
            neighborhood.extend(back_paths.iter().cloned());
        }

        neighborhood
    }

    /// Get neighborhood as note names (for display/output purposes).
    // kept: name-based view over `get_neighborhood`, exercised by the graph tests.
    #[allow(dead_code)]
    pub fn get_neighborhood_names(&self, path: &str) -> HashSet<String> {
        self.get_neighborhood(path)
            .into_iter()
            .filter_map(|p| {
                Path::new(&p)
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .map(String::from)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_index_is_empty() {
        let index = GraphIndex::new();
        assert!(index.is_empty());
        assert_eq!(index.len(), 0);
    }

    #[test]
    fn test_add_note_with_links() {
        let mut index = GraphIndex::new();

        let links: HashSet<String> = ["Note B", "Note C"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links);

        assert_eq!(index.len(), 1);
        assert!(!index.is_empty());

        // Now use path to get forward links
        let forward = index.get_forward_links("Note A.md").unwrap();
        assert!(forward.contains("Note B"));
        assert!(forward.contains("Note C"));
        assert_eq!(forward.len(), 2);
    }

    #[test]
    fn test_backlinks_are_created() {
        let mut index = GraphIndex::new();

        let links: HashSet<String> = ["Note B"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links);

        // Note B should have a backlink from Note A's path
        let backlinks = index.get_backlinks("Note B").unwrap();
        assert!(backlinks.contains("Note A.md"));
    }

    #[test]
    fn test_update_note_removes_old_backlinks() {
        let mut index = GraphIndex::new();

        // First, Note A links to Note B
        let links1: HashSet<String> = ["Note B"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links1);

        // Verify backlink exists (backlinks now contain paths)
        assert!(index.get_backlinks("Note B").unwrap().contains("Note A.md"));

        // Now update Note A to link to Note C instead
        let links2: HashSet<String> = ["Note C"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links2);

        // Note B should no longer have backlink from Note A
        let backlinks_b = index.get_backlinks("Note B");
        assert!(backlinks_b.is_none() || !backlinks_b.unwrap().contains("Note A.md"));

        // Note C should have backlink from Note A
        assert!(index.get_backlinks("Note C").unwrap().contains("Note A.md"));
    }

    #[test]
    fn test_remove_note() {
        let mut index = GraphIndex::new();

        let links: HashSet<String> = ["Note B"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links);

        index.remove_note("Note A", Path::new("Note A.md"));

        assert!(index.is_empty());
        assert!(index.get_forward_links("Note A.md").is_none());

        // Backlink from Note B should also be gone
        let backlinks = index.get_backlinks("Note B");
        assert!(backlinks.is_none() || backlinks.unwrap().is_empty());
    }

    #[test]
    fn test_get_neighborhood() {
        let mut index = GraphIndex::new();

        // Note A links to Note B and Note C
        let links_a: HashSet<String> = ["Note B", "Note C"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links_a);

        // Add Note B and Note C so their paths exist
        index.update_note("Note B", PathBuf::from("Note B.md"), HashSet::new());
        index.update_note("Note C", PathBuf::from("Note C.md"), HashSet::new());

        // Note D links to Note A
        let links_d: HashSet<String> = ["Note A"].iter().map(|s| s.to_string()).collect();
        index.update_note("Note D", PathBuf::from("Note D.md"), links_d);

        // get_neighborhood returns paths
        let neighborhood = index.get_neighborhood("Note A.md");

        // Should include forward links (B, C) and backlinks (D) - as paths
        assert!(neighborhood.contains("Note B.md"));
        assert!(neighborhood.contains("Note C.md"));
        assert!(neighborhood.contains("Note D.md"));
        assert_eq!(neighborhood.len(), 3);

        // get_neighborhood_names returns note names
        let names = index.get_neighborhood_names("Note A.md");
        assert!(names.contains("Note B"));
        assert!(names.contains("Note C"));
        assert!(names.contains("Note D"));
    }

    #[test]
    fn test_get_path() {
        let mut index = GraphIndex::new();

        index.update_note("Note A", PathBuf::from("folder/Note A.md"), HashSet::new());

        let path = index.get_path("Note A").unwrap();
        assert_eq!(path, PathBuf::from("folder/Note A.md"));
    }

    #[test]
    fn test_all_paths_iterator() {
        let mut index = GraphIndex::new();

        index.update_note("Note A", PathBuf::from("Note A.md"), HashSet::new());
        index.update_note("Note B", PathBuf::from("folder/Note B.md"), HashSet::new());

        let paths: HashSet<_> = index.all_paths().cloned().collect();
        assert!(paths.contains("Note A.md"));
        assert!(paths.contains("folder/Note B.md"));
        assert_eq!(paths.len(), 2);
    }

    #[test]
    fn test_embed_links_create_backlinks_but_not_phantom_paths() {
        // Belt-and-suspenders for c2's "no phantom note entries" guarantee: it
        // holds structurally at this layer regardless of the wiki-links
        // extractor fix, since `name_to_paths` is only ever populated from real
        // vault files (via `index_file`), never from a note's link targets.
        let mut index = GraphIndex::new();

        let links: HashSet<String> = ["diagram.png".to_string()].into_iter().collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links);

        assert!(
            index
                .get_backlinks("diagram.png")
                .unwrap()
                .contains("Note A.md")
        );
        assert!(index.get_paths_for_name("diagram.png").is_none());
    }

    #[test]
    fn test_backlinks_for_dotted_note_name() {
        // Belt-and-suspenders: GraphIndex itself is agnostic to dots in note
        // names — the whole bug lived in the extractor, not here.
        let mut index = GraphIndex::new();

        let links: HashSet<String> = ["llama.cpp".to_string()].into_iter().collect();
        index.update_note("Note A", PathBuf::from("Note A.md"), links);

        assert!(
            index
                .get_backlinks("llama.cpp")
                .unwrap()
                .contains("Note A.md")
        );
    }

    #[test]
    fn test_same_name_different_folders() {
        let mut index = GraphIndex::new();

        // Two notes with the same name in different folders
        index.update_note("Index", PathBuf::from("knowledge/Index.md"), HashSet::new());
        index.update_note("Index", PathBuf::from("projects/Index.md"), HashSet::new());

        // Both should exist
        assert_eq!(index.len(), 2);

        // get_paths_for_name should return both
        let paths = index.get_paths_for_name("Index").unwrap();
        assert_eq!(paths.len(), 2);
        assert!(paths.contains("knowledge/Index.md"));
        assert!(paths.contains("projects/Index.md"));

        // get_path returns one (for backward compat)
        let path = index.get_path("Index").unwrap();
        assert!(path.to_string_lossy().ends_with("Index.md"));
    }
}
