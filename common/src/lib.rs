pub mod bench;
pub mod config;
pub mod plot;
pub mod sensor;
pub mod util;

pub const MB_TO_MIB: f64 = 1_000_000.0 / 1_048_576.0;

pub const RUN_NONROOT: &str = "common/src/run-nonroot.sh";
