use std::{collections::HashMap, path::Path};

use common::{
    RUN_NONROOT,
    bench::{Bench, BenchArgs, Cmd, CmdsResult},
    config::{Config, Settings},
    util::{Filesystem, chown_user, mount_fs, simple_command_with_output_no_dir},
};
use eyre::Result;
use serde::{Deserialize, Serialize};
use tracing::debug;

pub mod result;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mlperf {
    pub model: Model,
    pub memory_gb: u16,
    pub n_accelerators: Vec<u8>,
    pub accelerator_type: AccelType,
    pub params: HashMap<String, String>,
    pub fs: Filesystem,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Model {
    #[default]
    None,
    Unet3d,
    Resnet50,
    Cosmoflow,
}

impl ToString for Model {
    fn to_string(&self) -> String {
        match self {
            Model::None => unreachable!(),
            Model::Unet3d => "unet3d".to_string(),
            Model::Resnet50 => "resnet50".to_string(),
            Model::Cosmoflow => "cosmoflow".to_string(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum AccelType {
    #[default]
    None,
    H100,
    A100,
}

impl ToString for AccelType {
    fn to_string(&self) -> String {
        match self {
            AccelType::None => unreachable!(),
            AccelType::H100 => "h100".to_string(),
            AccelType::A100 => "a100".to_string(),
        }
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MlperfConfig;

#[typetag::serde]
impl BenchArgs for MlperfConfig {
    fn name(&self) -> &'static str {
        "mlperf"
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for Mlperf {
    fn name(&self) -> &'static str {
        "mlperf"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(MlperfConfig)
    }

    fn runtime_estimate(&self) -> Result<u64> {
        Ok(900_000) // 15min
    }

    fn cmds(
        &self,
        _settings: &Settings,
        _bench_args: &dyn BenchArgs,
        _name: &str,
    ) -> Result<CmdsResult> {
        let cmds = self
            .n_accelerators
            .iter()
            .map(|n_accelerators| Mlperf {
                n_accelerators: vec![*n_accelerators],
                ..self.clone()
            })
            .enumerate()
            .map(|(idx, bench)| {
                let mut args = vec![
                    "mlpstorage".to_owned(),
                    "training".to_owned(),
                    "run".to_owned(),
                    "--model".to_owned(),
                    bench.model.to_string(),
                    "-cm".to_owned(),
                    bench.memory_gb.to_string(),
                    "-na".to_owned(),
                    bench.n_accelerators[0].to_string(),
                    "-g".to_owned(),
                    bench.accelerator_type.to_string(),
                ];

                for (k, v) in bench.params.iter() {
                    args.push("--param".to_owned());
                    args.push(format!("{k}={v}"));
                }

                Cmd {
                    args,
                    idx,
                    bench_obj: Box::new(bench.clone()),
                }
            })
            .collect();

        Ok(CmdsResult {
            program: RUN_NONROOT.to_owned(),
            cmds,
        })
    }

    fn add_path_args(&self, args: &mut Vec<String>, final_results_dir: &Path) {
        let mountpoint = final_results_dir
            .parent()
            .unwrap()
            .join("mountpoint")
            .to_string_lossy()
            .to_string();
        args.push("--data-dir".to_owned());
        args.push(mountpoint.clone());
        args.push("--results-dir".to_owned());
        args.push(final_results_dir.to_string_lossy().to_string());
        args.push("--checkpoint-folder".to_owned());
        args.push(mountpoint);
    }

    async fn experiment_init(
        &self,
        data_dir: &Path,
        settings: &Settings,
        _bench_args: &dyn BenchArgs,
        last_experiment: &Option<Box<dyn Bench>>,
        _config: &Config,
        _final_results_dir: &Path,
    ) -> Result<()> {
        let mlperf_mount = data_dir.join("mountpoint");
        let should_format = mlperf_mount.exists() && last_experiment.is_some();

        mount_fs(
            &mlperf_mount,
            &settings.device,
            &self.fs,
            !should_format,
            None::<String>,
        )
        .await?;
        chown_user(&mlperf_mount).await?;

        if should_format {
            debug!("No datagen required!");
            return Ok(());
        }

        let mut init_args = vec![
            "mlpstorage".to_owned(),
            "training".to_owned(),
            "datagen".to_owned(),
            "--model".to_owned(),
            self.model.to_string(),
            "-np".to_owned(),
            num_cpus::get().to_string(),
            "--data-dir".to_owned(),
            mlperf_mount.to_string_lossy().to_string(),
        ];

        for (k, v) in self.params.iter() {
            init_args.push("--param".to_owned());
            init_args.push(format!("{k}={v}"));
        }

        debug!("run-nonroot.sh {}", init_args.join(" "));
        _ = simple_command_with_output_no_dir(
            RUN_NONROOT,
            &init_args.iter().map(|x| x.as_str()).collect::<Vec<_>>(),
        )
        .await?;
        Ok(())
    }

    async fn post_experiment(
        &self,
        _data_dir: &Path,
        _final_results_dir: &Path,
        settings: &Settings,
        _bench_args: &dyn BenchArgs,
    ) -> Result<()> {
        _ = simple_command_with_output_no_dir("umount", &[&settings.device]).await?;
        Ok(())
    }
}
