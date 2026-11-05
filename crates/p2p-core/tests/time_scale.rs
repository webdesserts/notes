//! End-to-end proof that the process-global `set_time_scale` seed reaches the
//! global read path (`time_scale` / `scaled`) — the seam the pure `_with` unit
//! tests deliberately avoid.
//!
//! This test seeds the global `set_time_scale`, so it lives in its OWN
//! integration-test file: each integration test compiles to a separate binary,
//! so the first-wins `OnceLock` is naturally isolated to this process and cannot
//! collide with the parallel unit tests (which use the pure `_with` helpers) or
//! any other integration file.

use std::time::Duration;

use p2p_core::time_scale::{scaled, set_time_scale, time_scale};

/// At scale 0.1 a 30s base duration read through the *global* `scaled` collapses
/// to ~3s. The inline unit tests only exercise the pure `_with` helpers with an
/// explicit scale; this is the only coverage that the once-seeded global is what
/// `scaled`/`time_scale` actually read, so it proves the first-wins seed reaches
/// a real production read path.
#[test]
fn time_scale_seed_reaches_global_scaled() {
    // Seed before reading any scaled duration. First-wins; this binary's process
    // is dedicated to this test, so the seed is uncontended.
    assert!(
        set_time_scale(0.1),
        "set_time_scale should win on first call in a dedicated test binary"
    );

    // The seeded scale is what the global read returns — not the 1.0 default.
    assert_eq!(time_scale(), 0.1, "global scale should reflect the seed");

    // A duration read through the global `scaled` collapses by the seeded scale:
    // 30s → ~3s. Unscaled this would stay 30s, so the shrink is the proof the
    // seed reached the global read path.
    assert_eq!(
        scaled(Duration::from_secs(30)),
        Duration::from_secs(3),
        "30s should collapse to 3s under a 0.1 global time-scale"
    );
}
