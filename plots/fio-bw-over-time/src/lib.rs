use std::{
    collections::HashMap,
    path::{Path, PathBuf},
};

use common::{
    bench::BenchmarkInfo,
    config::{Config, Settings},
    plot::{Plot, PlotType},
};
use eyre::{ContextCompat, Result, bail};
use fio::Fio;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tokio::fs::create_dir_all;

