//! Type definitions for project discovery

use std::path::PathBuf;

/// Project metadata extracted from note frontmatter
#[derive(Debug, Clone)]
pub struct ProjectMetadata {
    /// Project note name (without .md extension)
    pub name: String,
    /// Absolute file path to the project note
    pub file_path: PathBuf,
    /// Current expected git remotes for this project
    pub remotes: Option<Vec<String>>,
    /// Previous git remotes (for detecting renames/disconnects)
    pub old_remotes: Option<Vec<String>>,
    /// Current directory name matcher (case-insensitive)
    pub slug: Option<String>,
    /// Previous directory names (for detecting renames/disconnects)
    pub old_slugs: Option<Vec<String>>,
}

/// Type of match discovered during project discovery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchType {
    /// Current remote or slug matched - auto-load silently
    Strict,
    /// Old remote or old slug matched - prompt to update
    Loose,
}

/// What field was matched during discovery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MatchedOn {
    Remote,
    Slug,
    OldRemote,
    OldSlug,
}

impl MatchedOn {
    pub fn as_str(&self) -> &'static str {
        match self {
            MatchedOn::Remote => "remote",
            MatchedOn::Slug => "slug",
            MatchedOn::OldRemote => "old_remote",
            MatchedOn::OldSlug => "old_slug",
        }
    }
}

/// Information about a single discovered project
// kept: `match_type`, `matched_value`, and `depth` are populated with real discovery
// context that status-message rendering may surface; deleting them would drop data the
// model is meant to carry.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DiscoveredProject {
    /// Project metadata from frontmatter
    pub metadata: ProjectMetadata,
    /// Type of match
    pub match_type: MatchType,
    /// What matched (e.g., 'remote', 'slug', 'old_remote', 'old_slug')
    pub matched_on: Option<MatchedOn>,
    /// The actual value that matched
    pub matched_value: Option<String>,
    /// Directory depth (0 = CWD, 1 = parent, etc.)
    pub depth: usize,
}

/// Result of project discovery for a working directory
// kept: `cwd` and `searched_paths` record the discovery context; populated deliberately
// and available to status-message rendering even though no consumer reads them yet.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct DiscoveryResult {
    /// Current working directory
    pub cwd: PathBuf,
    /// Git remotes found across all searched directories
    pub git_remotes: Vec<String>,
    /// All directories checked (CWD → parents up to home)
    pub searched_paths: Vec<PathBuf>,
    /// Projects discovered with strict matches
    pub strict_matches: Vec<DiscoveredProject>,
    /// Projects discovered with loose matches (disconnects)
    pub loose_matches: Vec<DiscoveredProject>,
    /// Suggested similar projects if no matches
    pub suggestions: Vec<ProjectMetadata>,
}

/// Directory information extracted during discovery
#[derive(Debug, Clone)]
pub struct DirectoryInfo {
    /// Absolute path to directory
    pub path: PathBuf,
    /// Directory basename
    pub name: String,
    /// Git remotes for this directory (empty if not a git repo)
    pub git_remotes: Vec<String>,
    /// Depth from CWD (0 = CWD, 1 = parent, etc.)
    pub depth: usize,
}
