use std::path::PathBuf;

/// Server configuration for the Obsidian vault.
#[derive(Debug, Clone)]
pub struct Config {
    /// Path to the Obsidian vault root directory
    pub vault_path: PathBuf,
    /// Name of the vault (derived from vault_path)
    pub vault_name: String,
}

impl Config {
    /// Create configuration from a vault path (with tilde expansion).
    pub fn new(vault_path: &str) -> Self {
        let vault_path = memory_common::expand_tilde(vault_path);
        let vault_name = vault_path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("vault")
            .to_string();

        Self {
            vault_path,
            vault_name,
        }
    }
}
