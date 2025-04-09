use core::fmt::Debug;
use std::{collections::HashMap, path::Path};

use downcast_rs::{Downcast, impl_downcast};
use dyn_clone::{DynClone, clone_trait_object};
use eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::{
    bench::BenchmarkInfo,
    config::{Config, Settings},
};

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
        info: &HashMap<String, BenchmarkInfo>,
        dirs: Vec<String>,
        settings: &Settings,
        completed_dirs: &mut Vec<String>,
    ) -> Result<()>;
}
clone_trait_object!(Plot);
impl_downcast!(Plot);

pub async fn plot(
    plots: &Option<Vec<Box<dyn Plot>>>,
    plot_type: PlotType,
    data_path: &Path,
    plot_path: &Path,
    config_yaml: &Config,
    info: &HashMap<String, BenchmarkInfo>,
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
