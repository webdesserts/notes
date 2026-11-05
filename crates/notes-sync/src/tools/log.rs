use chrono::{DateTime, Datelike, Local, Timelike, Weekday};
use rmcp::model::{CallToolResult, Content, ErrorData};
use std::path::Path;
use tokio::fs;

use super::log_format::{
    LogSection, parse_entry_time_24h, parse_log_sections, render_log_sections,
};

/// Format ISO week date as YYYY-Www-D (e.g., 2025-W48-1)
/// Uses chrono's IsoWeek trait
fn format_iso_week_date(dt: &DateTime<Local>) -> String {
    let iso_week = dt.iso_week();
    let weekday = dt.weekday().num_days_from_monday() + 1; // 1=Mon, 7=Sun
    format!("{}-W{:02}-{}", iso_week.year(), iso_week.week(), weekday)
}

/// Get 3-letter abbreviation for a weekday.
fn weekday_abbreviation(weekday: Weekday) -> &'static str {
    match weekday {
        Weekday::Mon => "Mon",
        Weekday::Tue => "Tue",
        Weekday::Wed => "Wed",
        Weekday::Thu => "Thu",
        Weekday::Fri => "Fri",
        Weekday::Sat => "Sat",
        Weekday::Sun => "Sun",
    }
}

/// Get 3-letter day abbreviation from DateTime (Mon, Tue, etc.)
fn get_day_abbreviation(dt: &DateTime<Local>) -> &'static str {
    weekday_abbreviation(dt.weekday())
}

/// Get day abbreviation from ISO week date (e.g., "2025-W50-1" -> "Mon")
pub fn get_day_abbreviation_from_iso(iso_week_date: &str) -> &'static str {
    // Parse the day number from YYYY-Www-D format (1=Mon, 7=Sun)
    let parts: Vec<&str> = iso_week_date.split('-').collect();
    if parts.len() == 3
        && let Ok(day) = parts[2].parse::<u32>()
    {
        // chrono's num_days_from_monday is 0-indexed, ISO week day is 1-indexed
        if let Ok(weekday) = Weekday::try_from(day as u8 - 1) {
            return weekday_abbreviation(weekday);
        }
    }
    "???"
}

/// Format time as 12-hour clock (h:MM AM/PM)
fn format_12_hour_time(dt: &DateTime<Local>) -> String {
    let hour = dt.hour();
    let minute = dt.minute();
    let (hour_12, am_pm) = if hour == 0 {
        (12, "AM")
    } else if hour < 12 {
        (hour, "AM")
    } else if hour == 12 {
        (12, "PM")
    } else {
        (hour - 12, "PM")
    };
    format!("{}:{:02} {}", hour_12, minute, am_pm)
}

/// Add a new entry to the log file, organizing by day and sorting chronologically.
///
/// Parses the file through `parse_log_sections` first, which merges any duplicate
/// day sections that P2P sync may have created.
pub async fn add_log(
    log_path: &Path,
    time: DateTime<Local>,
    entry: &str,
) -> Result<(String, String), std::io::Error> {
    let iso_week_date = format_iso_week_date(&time);
    let day_abbrev = get_day_abbreviation(&time);
    let time_str = format_12_hour_time(&time);

    // Format the new entry — strip leading dash if present
    let bullet_content = entry.strip_prefix('-').map(|s| s.trim()).unwrap_or(entry);
    let new_entry = format!("- {} – {}", time_str, bullet_content);

    // Read and parse existing log (merges duplicate sections)
    let log_content = match fs::read_to_string(log_path).await {
        Ok(content) => content,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(e) => return Err(e),
    };

    let (preamble, mut sections) = parse_log_sections(&log_content);
    let day_header = format!("## {} ({})", iso_week_date, day_abbrev);

    // Find or create the section for this day
    if let Some(section) = sections.iter_mut().find(|s| s.header == day_header) {
        section.entries.push(new_entry);
    } else {
        sections.push(LogSection {
            header: day_header,
            entries: vec![new_entry],
        });
    }

    // parse_log_sections sorts on parse, but we just pushed a new entry — re-sort
    if let Some(section) = sections
        .iter_mut()
        .find(|s| s.header == format!("## {} ({})", iso_week_date, day_abbrev))
    {
        section.entries.sort_by(|a, b| {
            let ta = parse_entry_time_24h(a).unwrap_or((24, 0));
            let tb = parse_entry_time_24h(b).unwrap_or((24, 0));
            ta.cmp(&tb)
        });
    }

    let content = render_log_sections(&preamble, &sections);
    fs::write(log_path, content).await?;

    Ok((iso_week_date, time_str))
}

/// Execute the Log tool
pub async fn execute(vault_path: &Path, content: &str) -> Result<CallToolResult, ErrorData> {
    let log_path = vault_path.join("Log.md");
    let now = Local::now();

    match add_log(&log_path, now, content).await {
        Ok((iso_week_date, time_str)) => {
            let text = format!("Logged at {} {}", iso_week_date, time_str);
            Ok(CallToolResult::success(vec![Content::text(text)]))
        }
        Err(e) => Err(ErrorData::internal_error(
            format!("Failed to write log: {}", e),
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use tempfile::TempDir;

    fn make_time(hour: u32, minute: u32) -> DateTime<Local> {
        Local
            .with_ymd_and_hms(2025, 12, 29, hour, minute, 0)
            .unwrap()
    }

    #[test]
    fn test_format_iso_week_date() {
        let dt = make_time(10, 0);
        let result = format_iso_week_date(&dt);
        // 2025-12-29 is Monday of week 1 of 2026 (ISO week dating)
        assert!(result.starts_with("2026-W01-1") || result.starts_with("2025-W52-"));
    }

    #[test]
    fn test_format_12_hour_time() {
        assert_eq!(format_12_hour_time(&make_time(0, 30)), "12:30 AM");
        assert_eq!(format_12_hour_time(&make_time(9, 5)), "9:05 AM");
        assert_eq!(format_12_hour_time(&make_time(12, 0)), "12:00 PM");
        assert_eq!(format_12_hour_time(&make_time(15, 30)), "3:30 PM");
        assert_eq!(format_12_hour_time(&make_time(23, 59)), "11:59 PM");
    }

    #[tokio::test]
    async fn test_add_log_creates_file_if_not_exists() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("Log.md");

        let time = make_time(10, 30);
        let (iso_week_date, time_str) = add_log(&log_path, time, "Test entry").await.unwrap();

        assert!(!iso_week_date.is_empty());
        assert_eq!(time_str, "10:30 AM");

        let content = fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("Test entry"));
        assert!(content.contains("10:30 AM"));
    }

    #[tokio::test]
    async fn test_add_log_appends_to_existing_section() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("Log.md");

        let time1 = make_time(9, 0);
        add_log(&log_path, time1, "First entry").await.unwrap();

        let time2 = make_time(11, 0);
        add_log(&log_path, time2, "Second entry").await.unwrap();

        let content = fs::read_to_string(&log_path).await.unwrap();
        assert!(content.contains("First entry"));
        assert!(content.contains("Second entry"));

        // Verify order - first entry should come before second
        let first_pos = content.find("First entry").unwrap();
        let second_pos = content.find("Second entry").unwrap();
        assert!(first_pos < second_pos);
    }

    #[tokio::test]
    async fn test_add_log_merges_duplicate_day_sections() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("Log.md");

        // Simulate P2P sync creating duplicate sections
        let content = "\
## 2026-W01-1 (Mon)

- 9:00 AM – Morning task

## 2026-W01-1 (Mon)

- 2:00 PM – Afternoon task
";
        fs::write(&log_path, content).await.unwrap();

        // Add a new entry for the same day (Mon 2025-12-29 = 2026-W01-1)
        let time = make_time(11, 0);
        add_log(&log_path, time, "Midday task").await.unwrap();

        let result = fs::read_to_string(&log_path).await.unwrap();

        // Should have exactly one section header
        assert_eq!(
            result.matches("## 2026-W01-1 (Mon)").count(),
            1,
            "duplicate sections should be merged"
        );

        // All three entries should be present in chronological order
        let morning = result.find("Morning task").unwrap();
        let midday = result.find("Midday task").unwrap();
        let afternoon = result.find("Afternoon task").unwrap();
        assert!(morning < midday, "morning before midday");
        assert!(midday < afternoon, "midday before afternoon");
    }

    #[tokio::test]
    async fn test_add_log_maintains_chronological_order() {
        let temp_dir = TempDir::new().unwrap();
        let log_path = temp_dir.path().join("Log.md");

        // Add entries out of order
        let time2 = make_time(14, 0);
        add_log(&log_path, time2, "Afternoon entry").await.unwrap();

        let time1 = make_time(9, 0);
        add_log(&log_path, time1, "Morning entry").await.unwrap();

        let content = fs::read_to_string(&log_path).await.unwrap();

        // Morning should come before afternoon
        let morning_pos = content.find("Morning entry").unwrap();
        let afternoon_pos = content.find("Afternoon entry").unwrap();
        assert!(
            morning_pos < afternoon_pos,
            "Morning entry should come before afternoon entry"
        );
    }
}
