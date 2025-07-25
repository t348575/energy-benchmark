use std::{collections::HashMap, path::PathBuf};

use clap::{Parser, Subcommand};
use common::{bench::BenchmarkInfo, config::Config, plot::PlotType};
use eyre::Result;
use regex::Regex;
use tokio::fs::{create_dir_all, read_dir, read_to_string, remove_dir_all};
use tracing::error;
use tracing_subscriber::{
    EnvFilter,
    fmt::{layer, time::ChronoLocal},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

mod bench;

#[derive(Parser)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    #[arg(long, default_value_t = false)]
    no_progress: bool,
    #[arg(short, long)]
    log: Vec<String>,
}

#[derive(Subcommand)]
enum Commands {
    /// List run benchmarks
    Ls,
    /// Run a benchmark
    Bench {
        #[arg(short, long, default_value = "config.yaml")]
        config_file: String,
        /// Do not generate plots
        #[arg(long, default_value_t = false)]
        skip_plot: bool,
    },
    /// Generate plots for benchmarks
    Plot {
        /// Benchmark folder
        #[arg(short, long)]
        folder: String,
    },
    /// Print generated benchmark commands
    Print {
        /// Benchmark folder
        #[arg(short, long)]
        folder: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let modules: &[&str] = macros::plugin_names_str!();
    let log_level = std::env::var("RUST_LOG").unwrap_or("warn".to_owned());
    let args = Cli::parse();
    let file_appender = tracing_appender::rolling::never(".", "log.log");
    let (non_blocking, _guard) = tracing_appender::non_blocking(file_appender);

    let mut env_filter = EnvFilter::new(format!("energy_benchmark={log_level}"));

    if !args.log.is_empty() {
        for log in &args.log {
            env_filter = env_filter.add_directive(log.parse()?);
        }
    }

    for module in modules {
        if !args.log.iter().any(|x| x.starts_with(module)) {
            env_filter = env_filter.add_directive(format!("{module}={log_level}").parse()?);
        }
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

    default_benches::init_benches();
    default_sensors::init_sensors();
    default_plots::init_plots();

    create_dir_all("results").await?;
    match args.command {
        Commands::Ls => list_benchmarks().await?,
        Commands::Bench {
            config_file,
            skip_plot,
        } => {
            if let Err(err) = bench::run_benchmark(config_file, args.no_progress, skip_plot).await {
                error!("{err:#?}");
                return Err(err);
            }
        }
        Commands::Plot { folder } => plot(&folder).await?,
        Commands::Print { folder } => print_commands(&folder).await?,
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

async fn print_commands(folder: &str) -> Result<()> {
    let base_path = PathBuf::from(folder);
    let config: Config =
        serde_yml::from_str(&read_to_string(base_path.join("config.yaml")).await?)?;

    for experiment in &config.benches {
        fn get_bench_args(
            bench_args: &[Box<dyn common::bench::BenchArgs>],
            bench: &dyn common::bench::Bench,
        ) -> Box<dyn common::bench::BenchArgs> {
            for args in bench_args {
                if args.name() == bench.name() {
                    return args.clone();
                }
            }
            bench.default_bench_args()
        }

        let bench_args = get_bench_args(&config.bench_args, &*experiment.bench);
        let commands = experiment
            .bench
            .cmds(&config.settings, &*bench_args, &experiment.name)?;
        println!("Commands: {commands:#?}");
    }
    Ok(())
}

async fn plot(folder: &str) -> Result<()> {
    let base_path = PathBuf::from(folder);
    let plot_path = base_path.join("plots");
    _ = remove_dir_all(&plot_path).await;
    create_dir_all(&plot_path).await?;
    let config: Config =
        serde_yml::from_str(&read_to_string(base_path.join("config.yaml")).await?)?;
    let data_path = base_path.join("data");

    let benchmark_info: HashMap<String, BenchmarkInfo> =
        serde_json::from_str(&read_to_string(base_path.join("info.json")).await?)?;

    for experiment in &config.benches {
        let dir_regex = Regex::new(&format!("^{}-ps(?:-1|[0-4])-\\S+$", experiment.name))?;
        let experiment_dirs = benchmark_info
            .keys()
            .filter(|x| dir_regex.is_match(x))
            .map(|x| x.to_owned())
            .collect::<Vec<_>>();

        common::plot::plot(
            &experiment.plots,
            PlotType::Individual,
            &data_path,
            &plot_path,
            &config,
            &benchmark_info,
            experiment_dirs.clone(),
            &config.settings,
            &mut Vec::new(),
        )
        .await?;
    }

    let mut completed_dirs = Vec::new();
    for experiment in &config.benches {
        common::plot::plot(
            &experiment.plots,
            PlotType::Total,
            &data_path,
            &plot_path,
            &config,
            &benchmark_info,
            benchmark_info.keys().cloned().collect(),
            &config.settings,
            &mut completed_dirs,
        )
        .await?;
    }

    Ok(())
}
