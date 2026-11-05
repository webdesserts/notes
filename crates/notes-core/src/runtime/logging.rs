use std::path::Path;

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize tracing with sensible defaults.
///
/// Respects `RUST_LOG` env var. Falls back to `info` level (or `debug` when verbose).
/// Module-specific filtering uses the provided `module_name` for targeted debug output.
pub fn init_tracing(verbose: bool, module_name: &str) {
    let default_filter = if verbose {
        format!("debug,{}=debug", module_name)
    } else {
        format!("info,{}=info", module_name)
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

/// Initialize tracing with a dual sink: stderr and a file at `log_path`.
///
/// The file log uses no ANSI color codes, making it suitable for agent consumption.
/// The directory containing `log_path` must already exist before calling this function.
///
/// Returns a `WorkerGuard` that MUST be held alive for the entire process lifetime.
/// Dropping the guard flushes and closes the background log-writer thread — any buffered
/// log lines not yet written to disk are dropped at that point. Hold it as `let _guard`
/// at the top of `main()` so it outlives the blocking `tauri.run()` or equivalent call.
///
/// Respects `RUST_LOG` env var. Falls back to `info` level (or `debug` when verbose).
pub fn init_tracing_with_file(verbose: bool, module_name: &str, log_path: &Path) -> WorkerGuard {
    let default_filter = if verbose {
        format!("debug,{}=debug", module_name)
    } else {
        format!("info,{}=info", module_name)
    };
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_filter));

    // Extract directory and filename from the provided path. Both are required by
    // rolling::never — panic here rather than silently writing to the wrong place.
    let log_dir = log_path
        .parent()
        .expect("log_path must have a parent directory");
    let log_filename = log_path.file_name().expect("log_path must have a filename");

    let file_appender = tracing_appender::rolling::never(log_dir, log_filename);
    let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(non_blocking),
        )
        .try_init()
        // try_init returns Err if a global subscriber is already set (e.g. in tests).
        // That's safe to ignore — the existing subscriber handles logging, and we still
        // return the guard so the caller can flush the writer on drop.
        .ok();

    guard
}

#[cfg(test)]
mod tests {
    use std::io::Read;

    use tempfile::TempDir;
    use tracing_subscriber::layer::SubscriberExt;

    /// Verifies that `init_tracing_with_file` writes log events to disk.
    ///
    /// We avoid relying on the global subscriber (set by try_init) because another test
    /// in the binary may have already claimed it, which would make the file write a
    /// silent no-op and cause a false pass or false fail. Instead, we build a local
    /// subscriber with `with_default` so the test is hermetic regardless of test
    /// ordering or parallelism.
    #[test]
    fn file_sink_writes_log_event() {
        let tmp = TempDir::new().expect("tempdir");
        let log_path = tmp.path().join("test.log");

        // Build the file appender and non-blocking writer directly, bypassing the
        // global init entirely. This is the same stack init_tracing_with_file wires
        // up in production, minus the registry().try_init() global registration.
        let file_appender =
            tracing_appender::rolling::never(tmp.path(), log_path.file_name().unwrap());
        let (non_blocking, guard) = tracing_appender::non_blocking(file_appender);

        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(non_blocking),
        );

        tracing::subscriber::with_default(subscriber, || {
            tracing::info!("test event marker");
        });

        // Drop the guard to flush the background writer thread before reading the file.
        drop(guard);

        let mut content = String::new();
        std::fs::File::open(&log_path)
            .expect("log file should exist")
            .read_to_string(&mut content)
            .expect("read log file");

        assert!(
            content.contains("test event marker"),
            "expected 'test event marker' in log file, got: {content:?}"
        );
    }
}
