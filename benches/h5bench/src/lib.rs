use std::{collections::HashMap, path::Path};

use common::{
    RUN_NONROOT,
    bench::{Bench, BenchArgs, Cmd, CmdsResult},
    config::{Config, Settings},
    util::{Filesystem, chown_user, mount_fs, simple_command_with_output_no_dir},
};
use eyre::{ContextCompat, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::fs::{copy, read_dir, write};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct H5Bench {
    pub rank: usize,
    pub benchmark: Benchmark,
    pub configuration: HashMap<String, String>,
    pub base_fs: Filesystem,
    #[cfg(feature = "prefill")]
    pub prefill: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "snake_case")]
pub enum Benchmark {
    #[default]
    None,
    Read,
    Write,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct H5BenchConfig {
    pub h5bench: String,
    pub hdf5: String,
}

#[typetag::serde]
impl BenchArgs for H5BenchConfig {
    fn name(&self) -> &'static str {
        "h5bench"
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for H5Bench {
    fn name(&self) -> &'static str {
        "h5bench"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(H5BenchConfig::default())
    }

    fn runtime_estimate(&self) -> Result<u64> {
        Ok(60_000)
    }

    fn cmds(
        &self,
        _settings: &Settings,
        bench_args: &dyn BenchArgs,
        _name: &str,
    ) -> Result<CmdsResult> {
        let bench_args = bench_args
            .downcast_ref::<H5BenchConfig>()
            .context("Invalid bench args")?;

        Ok(CmdsResult {
            program: RUN_NONROOT.to_owned(),
            cmds: vec![Cmd {
                args: vec![
                    Path::new(&bench_args.h5bench)
                        .join("bin/h5bench")
                        .to_string_lossy()
                        .to_string(),
                    "--debug".to_owned(),
                ],
                idx: 0,
                bench_obj: Box::new(self.clone()),
            }],
        })
    }

    fn add_path_args(&self, args: &mut Vec<String>, final_results_dir: &Path) {
        let configuration = final_results_dir
            .join("configuration.json")
            .to_string_lossy()
            .to_string();
        args.push(configuration);
    }

    fn add_env(&self, bench_args: &dyn BenchArgs) -> Result<HashMap<String, String>> {
        let bench_args = bench_args
            .downcast_ref::<H5BenchConfig>()
            .context("Invalid bench args")?;

        Ok(HashMap::from([(
            "LD_LIBRARY_PATH".to_owned(),
            format!("{}/lib:{}/lib", bench_args.h5bench, bench_args.hdf5),
        )]))
    }

    async fn experiment_init(
        &self,
        data_dir: &Path,
        settings: &Settings,
        _bench_args: &dyn BenchArgs,
        _last_experiment: &Option<Box<dyn Bench>>,
        config: &Config,
        final_results_dir: &Path,
    ) -> Result<()> {
        let mountpoint = data_dir.join("mountpoint");

        mount_fs(
            &mountpoint,
            &settings.device,
            &self.base_fs,
            !mountpoint.exists(),
            None::<String>,
        )
        .await?;
        chown_user(&mountpoint).await?;

        let mut mpi_configuration = match &settings.numa {
            Some(n) => format!(
                "numactl --cpunodebind={} --membind={}",
                n.cpunodebind, n.membind
            ),
            None => String::new(),
        };

        mpi_configuration = format!("-x LD_LIBRARY_PATH {mpi_configuration}");

        let mut conf = self.configuration.clone();
        conf.remove("csv_file");
        conf.remove("csv_file");
        conf.insert("CSV_FILE".to_owned(), "results.csv".to_owned());

        let h5bench_configuration = json!({
            "mpi": {
                "command": "mpirun",
                "ranks": self.rank,
                "configuration": mpi_configuration
            },
            "vol": {},
            "directory": mountpoint.to_string_lossy().to_string(),
            "file-system": {},
            "benchmarks": [{
                "benchmark": self.benchmark,
                "file": "test.h5",
                "configuration": conf
            }]
        });

        write(
            final_results_dir.join("configuration.json"),
            serde_json::to_string(&h5bench_configuration)?,
        )
        .await?;
        chown_user(data_dir).await?;

        #[cfg(feature = "prefill")]
        if let Some(size) = &self.prefill {
            let prefill_file = mountpoint.join("prefill");
            fio::Fio::prefill(&prefill_file, size, config, settings).await?;
        }
        Ok(())
    }

    async fn post_experiment(
        &self,
        data_dir: &Path,
        final_results_dir: &Path,
        settings: &Settings,
        _bench_args: &dyn BenchArgs,
    ) -> Result<()> {
        let mut dir = read_dir(data_dir.join("mountpoint")).await?;
        while let Ok(Some(e)) = dir.next_entry().await {
            if e.metadata().await?.is_dir() && e.file_name() != "lost+found" {
                copy(
                    e.path().join("results.csv"),
                    final_results_dir.join("results.csv"),
                )
                .await?;
                break;
            }
        }
        drop(dir);

        _ = simple_command_with_output_no_dir("umount", &[&settings.device]).await?;
        Ok(())
    }
}
