use core::fmt::Debug;
use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
};

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::{DynClone, clone_trait_object};
use eyre::{Result, eyre};
use futures::future::join_all;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    bench::{BenchInfo, BenchParams},
    config::{Config, Settings},
    util::plot_python,
};
use tokio::fs::create_dir_all;

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub enum PlotType {
    #[default]
    Individual,
    Total,
}

#[typetag::serde(tag = "type")]
#[async_trait::async_trait]
pub trait Plot: Debug + DynClone + Downcast + Send + Sync {
    /// The names of the sensors that this plot requires
    fn required_sensors(&self) -> &'static [&'static str];
    /// Plots the data
    ///
    /// Arguments:
    /// * `plot_type` - Individual or all config dirs passed
    /// * `data_path` - The path to the data, ie. /data
    /// * `plot_path` - The path to the plots, ie. /plots
    /// * `config_yaml` - The config yaml
    /// * `info` - Parsed info.json
    /// * `dirs` - The dirs for the experiment, or all dirs if [`PlotType::Total`]
    /// * `settings` - The settings from config_yaml
    /// * `completed_dirs` - The dirs that have already been plotted (useful for group plots from multiple experiments)
    async fn plot(
        &self,
        plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        config_yaml: &Config,
        info: &BenchInfo,
        dirs: Vec<String>,
        settings: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()>;
}
clone_trait_object!(Plot);
impl_downcast!(Plot);

#[derive(Debug, Clone)]
pub struct RunGroup {
    pub dir: String,
    pub info: BenchParams,
}

pub fn collect_run_groups(
    dirs: Vec<String>,
    info_map: &HashMap<String, BenchParams>,
    completed_dirs: &mut Vec<String>,
) -> Result<Vec<RunGroup>> {
    let mut unique = HashMap::new();
    for run in dirs {
        let info = info_map
            .get(&run)
            .ok_or_else(|| eyre!("No info for run {run}"))?;
        let key = (info.name.clone(), info.power_state, info.idx);
        if unique.contains_key(&key) {
            continue;
        }
        completed_dirs.push(run.clone());
        unique.insert(
            key,
            RunGroup {
                dir: run,
                info: info.clone(),
            },
        );
    }
    Ok(unique.into_values().collect())
}

pub async fn ensure_plot_dirs(dirs: &[PathBuf]) -> Result<()> {
    let create_jobs = dirs.iter().map(create_dir_all);
    for res in join_all(create_jobs).await {
        res?;
    }
    Ok(())
}

pub struct HeatmapJob<'a> {
    pub filepath: PathBuf,
    pub data: Vec<Vec<f64>>,
    pub title: &'a str,
    pub x_label: &'a str,
    pub reverse: bool,
}

pub fn render_heatmaps(
    experiment_name: &str,
    labels: &[String],
    plot_dir: &Path,
    jobs: &[HeatmapJob<'_>],
) -> Result<()> {
    if jobs.is_empty() {
        return Ok(());
    }

    if !plot_dir.exists() {
        fs::create_dir_all(plot_dir)?;
    }
    let plot_data_dir = plot_dir.join("plot_data");
    if !plot_data_dir.exists() {
        fs::create_dir_all(&plot_data_dir)?;
    }

    let labels_joined = labels.join(",");

    let jobs: Result<Vec<Vec<(String, String)>>> = jobs
        .iter()
        .map(|job| -> Result<Vec<(String, String)>> {
            if let Some(parent) = job.filepath.parent()
                && !parent.exists()
            {
                fs::create_dir_all(parent)?;
            }

            let stem = job
                .filepath
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| eyre!("Invalid filepath for heatmap: {:?}", job.filepath))?;
            let data_path = plot_data_dir.join(format!("{stem}.json"));
            fs::write(&data_path, serde_json::to_string(&job.data)?)?;

            let args = vec![
                ("--data".to_owned(), data_path.to_str().unwrap().to_owned()),
                (
                    "--filepath".to_owned(),
                    job.filepath
                        .to_str()
                        .ok_or_else(|| eyre!("Invalid filepath for heatmap"))?
                        .to_owned(),
                ),
                ("--col_labels".to_owned(), labels_joined.clone()),
                ("--x_label".to_owned(), job.x_label.to_owned()),
                ("--experiment_name".to_owned(), experiment_name.to_owned()),
                ("--title".to_owned(), job.title.to_owned()),
                (
                    "--reverse".to_owned(),
                    if job.reverse { "1" } else { "0" }.to_string(),
                ),
            ];
            Ok(args)
        })
        .collect();
    let jobs: Result<Vec<_>> = jobs?
        .into_par_iter()
        .map(|args| plot_python("efficiency", &args))
        .collect();
    jobs?;
    Ok(())
}

pub async fn plot(
    plots: &Option<Vec<Box<dyn Plot>>>,
    plot_type: PlotType,
    data_path: &Path,
    plot_path: &Path,
    config_yaml: &Config,
    info: &BenchInfo,
    dirs: Vec<String>,
    settings: &Settings,
    completed_dirs: &mut Vec<String>,
) -> Result<()> {
    if plots.is_none() {
        debug!("No plots");
        return Ok(());
    }

    let plots = plots.as_ref().unwrap();
    for plot in plots {
        plot.plot(
            &plot_type,
            data_path,
            plot_path,
            config_yaml,
            info,
            dirs.clone(),
            settings,
            completed_dirs,
        )
        .await?
    }
    Ok(())
}
