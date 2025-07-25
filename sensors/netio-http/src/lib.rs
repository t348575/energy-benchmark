use std::{
    cmp::min,
    time::{Duration, Instant},
};

use common::{
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, sensor_reader},
};
use eyre::{Context, ContextCompat, Result, eyre};
use flume::{Receiver, Sender};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::{spawn, task::JoinHandle, time::sleep};
use tracing::error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetioHttpConfig {
    pub url: String,
}

#[typetag::serde]
impl SensorArgs for NetioHttpConfig {
    fn name(&self) -> &'static str {
        "NetioHttp"
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetioHttp;

impl Sensor for NetioHttp {
    fn name(&self) -> &'static str {
        "NetioHttp"
    }

    fn start(
        &self,
        args: &dyn SensorArgs,
        rx: Receiver<SensorRequest>,
        tx: Sender<SensorReply>,
    ) -> Result<JoinHandle<Result<()>>> {
        let args = args
            .downcast_ref::<NetioHttpConfig>()
            .context("Invalid sensor args, expected args for NetioHttp")?;

        let args = args.clone();
        let handle = spawn(async move {
            if let Err(err) = sensor_reader(
                rx,
                tx,
                "netio-http",
                args,
                init_netio_http,
                |args: &NetioHttpConfig,
                 _,
                 _,
                 _|
                 -> std::pin::Pin<
                    Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
                > { Box::pin(read_netio_http(args.clone())) },
            )
            .await
            {
                error!("{err:#?}");
                return Err(err);
            }
            Ok(())
        });
        Ok(handle)
    }
}

async fn init_netio_http(_: NetioHttpConfig) -> Result<((), Vec<String>)> {
    let sensor_names = [
        "voltage",
        "current",
        "total_load",
        "output1_load",
        "output2_load",
    ];
    Ok(((), sensor_names.into_iter().map(|x| x.to_owned()).collect()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct NetioHttpResponse {
    global_measure: GlobalMeasure,
    outputs: Vec<Output>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct GlobalMeasure {
    voltage: f64,
    total_current: f64,
    total_load: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Output {
    load: f64,
}

async fn read_netio_http(args: NetioHttpConfig) -> Result<Vec<f64>, SensorError> {
    let start = Instant::now();
    let client = Client::new();
    let res: NetioHttpResponse = client
        .get(&args.url)
        .send()
        .await
        .context("Send request")
        .map_err(SensorError::MajorFailure)?
        .json()
        .await
        .context("Parse JSON")
        .map_err(SensorError::MajorFailure)?;
    if res.outputs.len() < 2 {
        return Err(SensorError::MajorFailure(eyre!(
            "Expected 2 outputs, got {}",
            res.outputs.len()
        )));
    }
    sleep(Duration::from_millis(min(
        500 - start.elapsed().as_millis() as u64,
        500,
    )))
    .await;
    Ok(vec![
        res.global_measure.voltage,
        res.global_measure.total_current,
        res.global_measure.total_load,
        res.outputs[0].load,
        res.outputs[1].load,
    ])
}
