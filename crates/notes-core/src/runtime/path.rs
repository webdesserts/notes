use std::path::PathBuf;

/// Expand `~` or `~/` prefix to the user's home directory.
pub fn expand_tilde(path: &str) -> PathBuf {
    if path == "~" {
        dirs::home_dir().unwrap_or_else(|| PathBuf::from("~"))
    } else if let Some(rest) = path.strip_prefix("~/") {
        dirs::home_dir()
            .map(|home| home.join(rest))
            .unwrap_or_else(|| PathBuf::from(path))
    } else {
        PathBuf::from(path)
    }
}
