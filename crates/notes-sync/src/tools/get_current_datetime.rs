use chrono::{Datelike, Local, Timelike};
use rmcp::model::{CallToolResult, Content, ErrorData};

/// Get the current date and time in ISO format for timeline entries.
///
/// Returns ISO 8601 formatted datetime (YYYY-MM-DDTHH:MM) and additional context
/// like day of week for use in Working Memory timeline entries.
pub fn execute() -> Result<CallToolResult, ErrorData> {
    let now = Local::now();

    // Format datetime as YYYY-MM-DDTHH:MM
    let iso_datetime = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}",
        now.year(),
        now.month(),
        now.day(),
        now.hour(),
        now.minute()
    );

    // Get day of week
    let day_of_week = match now.weekday() {
        chrono::Weekday::Mon => "Monday",
        chrono::Weekday::Tue => "Tuesday",
        chrono::Weekday::Wed => "Wednesday",
        chrono::Weekday::Thu => "Thursday",
        chrono::Weekday::Fri => "Friday",
        chrono::Weekday::Sat => "Saturday",
        chrono::Weekday::Sun => "Sunday",
    };

    let text = format!(
        "Current datetime: {}\nDay of week: {}\n\nUse this timestamp when creating timeline entries in Working Memory:\n```markdown\n## {} - Session Summary\n- Your timeline entries here...\n```",
        iso_datetime, day_of_week, iso_datetime
    );

    Ok(CallToolResult::success(vec![Content::text(text)]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_returns_success() {
        let result = execute().expect("should succeed");
        assert!(!result.is_error.unwrap_or(false));
        assert!(!result.content.is_empty());
    }

    #[test]
    fn test_output_contains_datetime_format() {
        let result = execute().expect("should succeed");

        // Content is a Vec<Annotated<RawContent>>. Get text from first item.
        let text = result.content[0]
            .raw
            .as_text()
            .expect("Expected text content");

        // Should contain ISO datetime pattern
        assert!(text.text.contains("Current datetime:"));
        assert!(text.text.contains("Day of week:"));

        // Should contain a date in YYYY-MM-DD format
        let lines: Vec<&str> = text.text.lines().collect();
        let datetime_line = lines[0];
        // Pattern: "Current datetime: YYYY-MM-DDTHH:MM"
        assert!(datetime_line.len() > 20);
    }
}
