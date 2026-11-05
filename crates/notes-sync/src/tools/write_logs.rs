//! WriteLogs tool for bulk replacing a day's log entries.
//!
//! Used during memory consolidation to rewrite or summarize a day's logs.
//! Parses through `log_format` first, which merges any duplicate day sections
//! that P2P sync may have created.

use rmcp::model::{CallToolResult, Content, ErrorData};
use std::collections::HashMap;
use std::path::Path;
use tokio::fs;

use super::log::get_day_abbreviation_from_iso;
use super::log_format::{LogSection, parse_log_sections, render_log_sections};

/// Replace an entire day's log entries with new entries.
pub async fn execute(
    vault_path: &Path,
    iso_week_date: &str,
    entries: HashMap<String, String>,
) -> Result<CallToolResult, ErrorData> {
    // Validate ISO week date format
    if !is_valid_iso_week_date(iso_week_date) {
        return Err(ErrorData::invalid_params(
            format!(
                "Invalid ISO week date format: '{}'. Expected format: YYYY-Www-D (e.g., '2025-W50-1')",
                iso_week_date
            ),
            None,
        ));
    }

    // Validate all time entries
    let mut invalid_times = Vec::new();
    for time in entries.keys() {
        if parse_time_12h(time).is_none() {
            invalid_times.push(time.clone());
        }
    }

    if !invalid_times.is_empty() {
        return Err(ErrorData::invalid_params(
            format!(
                "Invalid time format(s): {}. Expected format: 'h:mm AM' or 'h:mm PM'",
                invalid_times.join(", ")
            ),
            None,
        ));
    }

    let log_path = vault_path.join("Log.md");
    let day_abbrev = get_day_abbreviation_from_iso(iso_week_date);
    let day_header = format!("## {} ({})", iso_week_date, day_abbrev);

    // Read and parse existing content (merges duplicate sections)
    let log_content = match fs::read_to_string(&log_path).await {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => {
            return Err(ErrorData::internal_error(
                format!("Failed to read Log.md: {}", e),
                None,
            ));
        }
    };

    let (preamble, mut sections) = parse_log_sections(&log_content);

    // If entries is empty, delete the target day section
    if entries.is_empty() {
        let had_section = sections.iter().any(|s| s.header == day_header);
        sections.retain(|s| s.header != day_header);

        let content = render_log_sections(&preamble, &sections);
        if let Err(e) = fs::write(&log_path, content).await {
            return Err(ErrorData::internal_error(
                format!("Failed to write Log.md: {}", e),
                None,
            ));
        }

        return if had_section {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "Deleted day section for {}",
                iso_week_date
            ))]))
        } else {
            Ok(CallToolResult::success(vec![Content::text(format!(
                "No entries to delete - day section {} does not exist",
                iso_week_date
            ))]))
        };
    }

    // Sort entries chronologically
    let mut sorted_entries: Vec<(String, String)> = entries.into_iter().collect();
    sorted_entries.sort_by(|a, b| {
        let time_a = parse_time_12h(&a.0).unwrap_or((24, 0));
        let time_b = parse_time_12h(&b.0).unwrap_or((24, 0));
        time_a.cmp(&time_b)
    });

    // Format new entries
    let new_entries: Vec<String> = sorted_entries
        .iter()
        .map(|(time, message)| format!("- {} – {}", time, message))
        .collect();

    // Replace or create the target section
    if let Some(section) = sections.iter_mut().find(|s| s.header == day_header) {
        section.entries = new_entries;
    } else {
        sections.push(LogSection {
            header: day_header,
            entries: new_entries,
        });
    }

    let content = render_log_sections(&preamble, &sections);
    if let Err(e) = fs::write(&log_path, content).await {
        return Err(ErrorData::internal_error(
            format!("Failed to write Log.md: {}", e),
            None,
        ));
    }

    Ok(CallToolResult::success(vec![Content::text(format!(
        "Replaced {} entries for {}",
        sorted_entries.len(),
        iso_week_date
    ))]))
}

/// Validate ISO week date format: YYYY-Www-D
fn is_valid_iso_week_date(s: &str) -> bool {
    // Check format: YYYY-Www-D
    let parts: Vec<&str> = s.split('-').collect();
    if parts.len() != 3 {
        return false;
    }

    // Year: 4 digits
    if parts[0].len() != 4 || parts[0].parse::<u32>().is_err() {
        return false;
    }

    // Week: Www where w is 01-53
    if !parts[1].starts_with('W') || parts[1].len() != 3 {
        return false;
    }
    match parts[1][1..].parse::<u32>() {
        Ok(w) if (1..=53).contains(&w) => {}
        _ => return false,
    };

    // Day: 1-7
    match parts[2].parse::<u32>() {
        Ok(d) if (1..=7).contains(&d) => {}
        _ => return false,
    };

    true
}

/// Parse 12-hour time format (e.g., "9:30 AM") to (hour24, minute)
fn parse_time_12h(s: &str) -> Option<(u32, u32)> {
    let parts: Vec<&str> = s.split_whitespace().collect();
    if parts.len() != 2 {
        return None;
    }

    let time_parts: Vec<&str> = parts[0].split(':').collect();
    if time_parts.len() != 2 {
        return None;
    }

    let hour: u32 = time_parts[0].parse().ok()?;
    let minute: u32 = time_parts[1].parse().ok()?;

    if !(1..=12).contains(&hour) || minute > 59 {
        return None;
    }

    let am_pm = parts[1].to_uppercase();
    let hour_24 = match am_pm.as_str() {
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
    use tempfile::TempDir;

    #[test]
    fn test_is_valid_iso_week_date() {
        assert!(is_valid_iso_week_date("2025-W50-1"));
        assert!(is_valid_iso_week_date("2025-W01-7"));
        assert!(is_valid_iso_week_date("2026-W52-3"));

        assert!(!is_valid_iso_week_date("2025-50-1")); // Missing W
        assert!(!is_valid_iso_week_date("2025-W54-1")); // Invalid week
        assert!(!is_valid_iso_week_date("2025-W50-8")); // Invalid day
        assert!(!is_valid_iso_week_date("2025-W50-0")); // Invalid day
        assert!(!is_valid_iso_week_date("invalid"));
    }

    #[test]
    fn test_parse_time_12h() {
        assert_eq!(parse_time_12h("9:30 AM"), Some((9, 30)));
        assert_eq!(parse_time_12h("12:00 PM"), Some((12, 0)));
        assert_eq!(parse_time_12h("12:00 AM"), Some((0, 0)));
        assert_eq!(parse_time_12h("3:45 PM"), Some((15, 45)));
        assert_eq!(parse_time_12h("11:59 PM"), Some((23, 59)));

        assert_eq!(parse_time_12h("13:00 PM"), None); // Invalid hour
        assert_eq!(parse_time_12h("9:60 AM"), None); // Invalid minute
        assert_eq!(parse_time_12h("9:30"), None); // Missing AM/PM
        assert_eq!(parse_time_12h("invalid"), None);
    }

    #[tokio::test]
    async fn test_write_logs_creates_new_section() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();
        let log_path = vault_path.join("Log.md");

        // Create empty log file
        fs::write(&log_path, "").await.unwrap();

        let mut entries = HashMap::new();
        entries.insert("9:00 AM".to_string(), "Started work".to_string());
        entries.insert("2:30 PM".to_string(), "Finished task".to_string());

        let result = execute(vault_path, "2025-W50-1", entries).await;
        assert!(result.is_ok());

        let content = fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("## 2025-W50-1 (Mon)"));
        assert!(content.contains("9:00 AM – Started work"));
        assert!(content.contains("2:30 PM – Finished task"));
    }

    #[tokio::test]
    async fn test_write_logs_replaces_existing_section() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();
        let log_path = vault_path.join("Log.md");

        // Create log with existing content
        let initial_content = "## 2025-W50-1 (Mon)\n\n- 8:00 AM – Old entry\n\n";
        fs::write(&log_path, initial_content).await.unwrap();

        let mut entries = HashMap::new();
        entries.insert("10:00 AM".to_string(), "New entry".to_string());

        let result = execute(vault_path, "2025-W50-1", entries).await;
        assert!(result.is_ok());

        let content = fs::read_to_string(&log_path).await.unwrap();
        assert!(!content.contains("Old entry"));
        assert!(content.contains("New entry"));
    }

    #[tokio::test]
    async fn test_write_logs_deletes_section_when_empty() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();
        let log_path = vault_path.join("Log.md");

        // Create log with content
        let initial_content = "## 2025-W50-1 (Mon)\n\n- 8:00 AM – Entry\n\n";
        fs::write(&log_path, initial_content).await.unwrap();

        let entries = HashMap::new(); // Empty = delete

        let result = execute(vault_path, "2025-W50-1", entries).await;
        assert!(result.is_ok());

        let content = fs::read_to_string(&log_path).await.unwrap();
        assert!(!content.contains("2025-W50-1"));
    }

    #[tokio::test]
    async fn test_write_logs_invalid_iso_date() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        let entries = HashMap::new();
        let result = execute(vault_path, "invalid", entries).await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_logs_invalid_time_format() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();

        let mut entries = HashMap::new();
        entries.insert("invalid".to_string(), "Message".to_string());

        let result = execute(vault_path, "2025-W50-1", entries).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_write_logs_sorts_chronologically() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();
        let log_path = vault_path.join("Log.md");

        fs::write(&log_path, "").await.unwrap();

        let mut entries = HashMap::new();
        entries.insert("3:00 PM".to_string(), "Third".to_string());
        entries.insert("9:00 AM".to_string(), "First".to_string());
        entries.insert("12:00 PM".to_string(), "Second".to_string());

        execute(vault_path, "2025-W50-1", entries).await.unwrap();

        let content = fs::read_to_string(&log_path).await.unwrap();
        let first_pos = content.find("First").unwrap();
        let second_pos = content.find("Second").unwrap();
        let third_pos = content.find("Third").unwrap();

        assert!(first_pos < second_pos);
        assert!(second_pos < third_pos);
    }

    #[tokio::test]
    async fn test_write_logs_merges_duplicate_sections() {
        let temp_dir = TempDir::new().unwrap();
        let vault_path = temp_dir.path();
        let log_path = vault_path.join("Log.md");

        // Simulate P2P sync creating duplicate Monday sections
        let content = "\
## 2025-W50-1 (Mon)

- 9:00 AM – Morning task

## 2025-W50-1 (Mon)

- 2:00 PM – Afternoon task
";
        fs::write(&log_path, content).await.unwrap();

        // Write entries for Tuesday — should heal Monday duplicates too
        let mut entries = HashMap::new();
        entries.insert("10:00 AM".to_string(), "Tuesday work".to_string());
        execute(vault_path, "2025-W50-2", entries).await.unwrap();

        let result = fs::read_to_string(&log_path).await.unwrap();

        // Monday should be merged into one section
        assert_eq!(
            result.matches("## 2025-W50-1 (Mon)").count(),
            1,
            "duplicate Monday sections should be merged"
        );

        // Both Monday entries should survive
        assert!(result.contains("Morning task"));
        assert!(result.contains("Afternoon task"));

        // Tuesday should exist
        assert!(result.contains("## 2025-W50-2 (Tue)"));
        assert!(result.contains("Tuesday work"));
    }
}
