use std::{collections::HashMap, path::PathBuf};

use clap::{Parser, Subcommand};
use common::{bench::BenchmarkInfo, config::Config};
use eyre::Result;
use tokio::fs::{create_dir_all, read_dir, read_to_string};
use tracing_subscriber::{
    EnvFilter,
    fmt::{layer, time::ChronoLocal},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};
use tracing::error;

mod bench;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// List run benchmarks
    Ls,
    /// Run a benchmark
    Bench {
        #[arg(short, long, default_value = "config.yaml")]
        config_file: String,
    },
    /// Generate plots for benchmarks
    Plot {
        /// Benchmark folder
        #[arg(short, long)]
        folder: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let modules = macros::plugin_names_str!();
    let log_level = std::env::var("RUST_LOG").unwrap_or("warn".to_owned());
    let file_appender = tracing_appender::rolling::never(".", "log.log".to_owned());
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let mut env_filter = EnvFilter::new(format!("energy_benchmark={log_level}"))
        .add_directive(format!("common={log_level}").parse()?);

    for module in modules {
        env_filter = env_filter.add_directive(format!("{module}={log_level}").parse()?);
    }

    tracing_subscriber::registry()
        .with(env_filter)
        .with(
            layer()
                .with_timer(ChronoLocal::new("%v %k:%M:%S %z".to_owned()))
                .compact(),
        )
        .with(layer().with_writer(non_blocking))
        .init();

    let args = Cli::parse();

    default_bench::init_benches();
    default_sensor::init_sensors();
    pyo3::prepare_freethreaded_python();

    create_dir_all("results").await?;
    match args.command {
        Commands::Ls => list_benchmarks().await?,
        Commands::Bench { config_file } => if let Err(err) = bench::run_benchmark(config_file).await {
            error!("{err:#?}");
            return Err(err);
        },
        Commands::Plot { folder } => plot(&folder).await?,
    };

    Ok(())
}

async fn list_benchmarks() -> Result<()> {
    for (name, folder) in get_benchmarks().await? {
        println!(
            "{} -> {}",
            name,
            folder.file_name().unwrap().to_str().unwrap()
        );
    }
    Ok(())
}

async fn get_benchmarks() -> Result<Vec<(String, PathBuf)>> {
    let mut items = read_dir("results").await?;
    let mut results = Vec::new();
    while let Ok(Some(entry)) = items.next_entry().await {
        if entry.file_type().await?.is_dir() {
            let config_file = entry.path().join("config.yaml");
            if config_file.exists() {
                let config: Config = serde_yml::from_str(&read_to_string(config_file).await?)?;
                results.push((config.name, entry.path()));
            }
        }
    }
    Ok(results)
}

async fn plot(folder: &str) -> Result<()> {
    let base_path = PathBuf::from(folder);
    let config: Config =
        serde_yml::from_str(&read_to_string(base_path.join("config.yaml")).await?)?;
    let data_path = base_path.join("data");

    let benchmark_info: HashMap<String, BenchmarkInfo> =
        serde_json::from_str(&read_to_string(base_path.join("info.json")).await?)?;

    for experiment in config.benches {
        let experiment_dirs = benchmark_info
            .keys()
            .filter(|x| x.starts_with(&experiment.name))
            .map(|x| x.to_owned())
            .collect::<Vec<_>>();

        experiment
            .bench
            .plot(
                &data_path,
                &base_path,
                &benchmark_info,
                experiment_dirs.clone(),
                &config.settings,
            )
            .await?;
    }
    Ok(())
}
