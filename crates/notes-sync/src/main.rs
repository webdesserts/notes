//! `notes` CLI binary — a thin wrapper over [`notes::cli`].

fn main() -> anyhow::Result<()> {
    notes::cli::run_from_os_args(std::env::args_os().collect())
}
