use std::path::Path;

use common::{
    bench::{Bench, BenchArgs, CmdsResult},
    config::Settings,
};
use eyre::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct Cmd {
    pub program: String,
    pub args: Option<Vec<String>>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct CmdConfig;

#[typetag::serde]
impl BenchArgs for CmdConfig {
    fn name(&self) -> &'static str {
        "cmd"
    }
}

#[async_trait::async_trait]
#[typetag::serde]
impl Bench for Cmd {
    fn name(&self) -> &'static str {
        "cmd"
    }

    fn default_bench() -> Box<dyn Bench> {
        Box::new(Self::default())
    }

    fn default_bench_args(&self) -> Box<dyn BenchArgs> {
        Box::new(CmdConfig)
    }

    fn runtime_estimate(&self) -> Result<u64> {
        Ok(0)
    }

    fn cmds(
        &self,
        _settings: &Settings,
        _bench_args: &dyn BenchArgs,
        _name: &str,
    ) -> Result<CmdsResult> {
        let args = self.args.clone().unwrap_or_default();
        let hash = format!("{:x}", md5::compute(args.join(" ")));
        Ok(CmdsResult {
            program: self.program.clone(),
            cmds: vec![common::bench::Cmd {
                args: args.clone(),
                hash,
                bench_obj: Box::new(Cmd {
                    program: self.program.clone(),
                    args: Some(args),
                }),
            }],
        })
    }

    async fn check_results(&self, _results_path: &Path, _dirs: &[String]) -> Result<Vec<usize>> {
        Ok(vec![])
    }
}
