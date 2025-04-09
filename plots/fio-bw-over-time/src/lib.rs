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

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FioBwOverTime {
    variable: String,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Plot for FioBwOverTime {
    fn required_sensors(&self) -> &'static [&'static str] {
        &["Powersensor3", "Pmt", "Sysinfo"]
    }

    async fn plot(
        &self,
        _plot_type: &PlotType,
        data_path: &Path,
        plot_path: &Path,
        config_yaml: &Config,
        info: &HashMap<String, BenchmarkInfo>,
        dirs: Vec<String>,
        _: &Settings,
        _: &mut Vec<String>,
    ) -> Result<()> {
        let mut groups = HashMap::new();
        for run in dirs {
            let item = info.get(&run).context("No info for run")?;
            let key = (item.name.clone(), item.power_state, item.hash.clone());
            groups.entry(key).or_insert_with(|| (run, item.clone()));
        }

        let entries = groups.drain().map(|(_, x)| x).collect::<Vec<_>>();

        let bw_dir = plot_path.join("bw_over_time");
        let bw_inner_dir = bw_dir.join(&entries[0].1.name);
        create_dir_all(&bw_inner_dir).await?;
        entries.par_iter().for_each(|data| {
            let config_yaml = config_yaml.benches.iter().find(|x| x.name.eq(&data.1.name));
            let config_yaml = config_yaml
                .as_ref()
                .unwrap()
                .bench
                .downcast_ref::<Fio>()
                .unwrap();
            self.bw_over_time(
                data_path.join(data.0.clone()),
                &bw_inner_dir,
                config_yaml,
                &data.1,
            )
            .expect("Error running bw_over_time");
        });
        Ok(())
    }
}

impl FioBwOverTime {
    fn bw_over_time(
        &self,
        data_path: PathBuf,
        plot_path: &Path,
        _config_yaml: &Fio,
        info: &BenchmarkInfo,
    ) -> Result<()> {
        let config = info.args.downcast_ref::<Fio>().unwrap();
        let variable = match self.variable.as_str() {
            "request_sizes" => config.request_sizes[0].clone(),
            "io_depths" => config.io_depths[0].to_string(),
            "num_jobs" => config.num_jobs.as_ref().unwrap()[0].to_string(),
            _ => bail!("Unsupported plot variable {}", self.variable),
        };
        let name = format!("{}-{}-{}", info.name, info.power_state, variable);
        let mut child = std::process::Command::new("python3")
            .args([
                "plots/bw_over_time.py",
                "--plot_dir",
                plot_path.to_str().unwrap(),
                "--results_dir",
                data_path.to_str().unwrap(),
                "--name",
                &name,
            ])
            .spawn()?;
        child.wait()?;
        Ok(())
    }
}
