use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::SystemTime,
};

use common::{
    bench::{Bench, BenchArgs, Cmd, CmdsResult},
    config::Settings,
    util::{Filesystem, mount_fs, simple_command_with_output, simple_command_with_output_no_dir},
};
use eyre::{ContextCompat, Result, bail};
use handlebars::Handlebars;
use serde::{Deserialize, Serialize};
use tokio::fs::{DirEntry, copy, read_dir, remove_file, write};
use tracing::debug;

pub mod result;

const DOCKER: &str = "docker";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TpccPostgres {
    pub num_clients: Vec<u8>,
    pub warehouses: usize,
    pub config_file: Option<String>,
    pub filesystem: Filesystem,
    pub fs_mount_opts: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct TpccPostgresConfig {
    pub benchhelpers: String,
    pub tpcc_postgres: String,
}

#[typetag::serde]
impl BenchArgs for TpccPostgresConfig {
    fn name(&self) -> &'static str {
        "tpc-postgres"
    }
}

#[derive(Serialize)]
struct ComposeContext {
    clients: Vec<String>,
    tpcc_postgres: String,
    benchhelpers: String,
    config_file: Option<String>,
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for TpccPostgres {
    fn name(&self) -> &'static str {
        "tpc-postgres"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(TpccPostgresConfig::default())
    }

    fn runtime_estimate(&self) -> Result<u64> {
        Ok(1_000_000 + 120_000) // 1000 seconds + 2 minutes
    }

    fn cmds(
        &self,
        _settings: &Settings,
        _bench_args: &dyn BenchArgs,
        _name: &str,
    ) -> Result<CmdsResult> {
        let cmds = self
            .num_clients
            .iter()
            .map(|num_clients| TpccPostgres {
                num_clients: vec![*num_clients],
                warehouses: self.warehouses,
                filesystem: self.filesystem.clone(),
                config_file: self.config_file.clone(),
                fs_mount_opts: self.fs_mount_opts.clone(),
            })
            .enumerate()
            .map(|(idx, bench)| Cmd {
                args: [
                    "exec",
                    "tpcc-host",
                    "/run.sh",
                    "--warehouses",
                    &bench.warehouses.to_string(),
                    "--run-phase-only",
                ]
                .into_iter()
                .map(|x| x.to_owned())
                .collect(),
                idx,
                bench_obj: Box::new(bench.clone()),
            })
            .collect();
        Ok(CmdsResult {
            program: "docker".to_owned(),
            cmds,
        })
    }

    async fn experiment_init(
        &self,
        data_dir: &Path,
        settings: &Settings,
        bench_args: &dyn BenchArgs,
        last_experiment: &Option<Box<dyn Bench>>,
    ) -> Result<()> {
        let common_dir = data_dir.join("common_dir");
        let postgres_mount = common_dir.join("mountpoint");
        let bench_args = bench_args
            .downcast_ref::<TpccPostgresConfig>()
            .context("Invalid bench args")?;

        let mut should_load = true;
        if postgres_mount.exists() && last_experiment.is_some() {
            let last_experiment = last_experiment
                .as_ref()
                .unwrap()
                .downcast_ref::<TpccPostgres>()
                .unwrap();
            if last_experiment.warehouses == self.warehouses {
                should_load = false;
            }
        }

        mount_fs(
            &postgres_mount,
            &settings.device,
            self.filesystem.clone(),
            should_load,
            self.fs_mount_opts.clone(),
        )
        .await?;
        let cwd = std::env::current_dir().unwrap();
        let context = ComposeContext {
            benchhelpers: cwd
                .join(&bench_args.benchhelpers)
                .to_string_lossy()
                .to_string(),
            tpcc_postgres: cwd
                .join(&bench_args.tpcc_postgres)
                .to_string_lossy()
                .to_string(),
            clients: (1..=self.num_clients[0])
                .map(|x| format!("tpcc-{x}"))
                .collect(),
            config_file: self.config_file.clone(),
        };
        let mut handlebars = Handlebars::new();
        handlebars.register_template_string(
            "docker_compose",
            include_str!("docker-compose.template.yaml"),
        )?;

        let rendered = handlebars.render("docker_compose", &context)?;
        write(common_dir.join("docker-compose.yaml"), rendered).await?;
        write(
            common_dir.join("hosts"),
            format!("{}\n", context.clients.join("\n")),
        )
        .await?;
        _ = simple_command_with_output(
            DOCKER,
            &["compose", "up", "-d"],
            &common_dir,
            &HashMap::new(),
        )
        .await?;

        let helper = InitHelper {
            dir: common_dir.clone(),
        };
        helper.exec("tpcc-host", &["/gen-key.sh"]).await?;
        helper
            .cp("tpcc-host:/root/.ssh/id_rsa.pub", "tpcc-host.pub")
            .await?;

        for i in 1..=self.num_clients[0] {
            helper
                .cp("tpcc-host.pub", &format!("tpcc-{i}:/tmp/id_rsa.pub"))
                .await?;
            helper.exec(&format!("tpcc-{i}"), &["/add-key.sh"]).await?;
        }

        remove_file(common_dir.join("tpcc-host.pub")).await?;
        helper
            .exec(
                "tpcc-host",
                &["/set-keys.sh", &self.num_clients[0].to_string()],
            )
            .await?;

        helper
            .exec(
                "tpcc-host",
                &[
                    "/benchhelpers/tpcc/ydb/upload_benchbase.sh",
                    "--package",
                    "/target/benchbase-postgres.tgz",
                    "--hosts",
                    "/hosts",
                    "--user",
                    "root",
                ],
            )
            .await?;

        if should_load {
            helper
                .exec(
                    "tpcc-host",
                    &[
                        "/run.sh",
                        "--warehouses",
                        &self.warehouses.to_string(),
                        "--no-run",
                    ],
                )
                .await?;
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
        _ = simple_command_with_output(
            DOCKER,
            &["compose", "down"],
            &data_dir.join("common_dir"),
            &HashMap::new(),
        )
        .await?;
        let last_dir = find_last_created_dir(&data_dir.join("common_dir/logs/postgres")).await?;
        match last_dir {
            Some(results) => {
                debug!(
                    "Copying results from {} to {}",
                    results.display(),
                    final_results_dir.display()
                );
                copy(
                    results.join("result.json"),
                    final_results_dir.join("result.json"),
                )
                .await?;
            }
            None => bail!("No tpcc-postgres logs found"),
        }
        _ = simple_command_with_output_no_dir("umount", &[&settings.device]).await?;
        Ok(())
    }
}

struct InitHelper {
    dir: PathBuf,
}

impl InitHelper {
    async fn cp(&self, from: &str, to: &str) -> Result<()> {
        let mut args = vec!["compose", "cp"];
        args.push(from);
        args.push(to);
        _ = simple_command_with_output(DOCKER, &args, &self.dir, &HashMap::new()).await?;
        Ok(())
    }

    async fn exec(&self, container: &str, args: &[&str]) -> Result<()> {
        let mut new_args = vec!["compose", "exec", container];
        new_args.extend(args);
        debug!("Running command: {new_args:?}");
        _ = simple_command_with_output(DOCKER, &new_args, &self.dir, &HashMap::new()).await?;
        Ok(())
    }
}

async fn is_dir(entry: &DirEntry) -> Result<bool> {
    let ft = entry.file_type().await?;
    Ok(ft.is_dir())
}

async fn find_last_created_dir(root_dir: &Path) -> Result<Option<PathBuf>> {
    let mut entries = read_dir(root_dir).await?;
    let mut latest: Option<(PathBuf, SystemTime)> = None;

    while let Some(entry) = entries.next_entry().await? {
        if is_dir(&entry).await? {
            if let Ok(metadata) = entry.metadata().await {
                let created = metadata
                    .created()
                    .unwrap_or_else(|_| metadata.modified().unwrap_or(SystemTime::UNIX_EPOCH));

                match &latest {
                    Some((_, last_time)) if created > *last_time => {
                        latest = Some((entry.path(), created));
                    }
                    None => {
                        latest = Some((entry.path(), created));
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(latest.map(|(path, _)| path))
}
