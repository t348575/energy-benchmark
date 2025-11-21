use std::{
    cmp::min,
    time::{Duration, Instant},
};

use common::{
    config::Settings,
    sensor::{Sensor, SensorArgs, SensorReply, SensorRequest},
    util::{SensorError, sensor_reader},
};
use eyre::{Context, ContextCompat, Result, eyre};
use flume::{Receiver, Sender};
use futures::future::try_join_all;
use reqwest::Client;
use sensor_common::SensorKind;
use serde::{Deserialize, Serialize};
use tokio::{spawn, task::JoinHandle, time::sleep};
use tracing::error;

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetioHttpConfig {
    pub pdus: Vec<NetioHttpPdu>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetioHttpPdu {
    pub alias: String,
    pub url: String,
    pub loads: Vec<String>,
}

#[typetag::serde]
impl SensorArgs for NetioHttpConfig {
    fn name(&self) -> SensorKind {
        SensorKind::NetioHttp
    }
}

const NETIO_FILENAME: &str = "netio-http.csv";

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct NetioHttp;

#[derive(Debug, Clone)]
struct InternalNetioHttp(usize);

impl Sensor for NetioHttp {
    fn name(&self) -> SensorKind {
        SensorKind::NetioHttp
    }

    fn filename(&self) -> &'static str {
        NETIO_FILENAME
    }

    fn start(
        &self,
        args: &dyn SensorArgs,
        _: &Settings,
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
                NETIO_FILENAME,
                args,
                init_netio_http,
                |args: &NetioHttpConfig,
                 s,
                 _,
                 _|
                 -> std::pin::Pin<
                    Box<dyn Future<Output = Result<Vec<f64>, SensorError>> + Send>,
                > { Box::pin(read_netio_http(s, args)) },
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

async fn init_netio_http(args: NetioHttpConfig) -> Result<(InternalNetioHttp, Vec<String>)> {
    let mut sensor_names = Vec::new();
    for pdu in args.pdus {
        sensor_names.push(format!("{}-voltage", pdu.alias));
        sensor_names.extend(
            pdu.loads
                .into_iter()
                .map(|x| {
                    [
                        format!("{}-current", pdu.alias),
                        format!("load-{}-{x}", pdu.alias),
                    ]
                })
                .flatten(),
        );
    }
    Ok((InternalNetioHttp(sensor_names.len()), sensor_names))
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
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
struct Output {
    load: f64,
    name: String,
    current: f64,
}

async fn read_netio_http(
    s: &InternalNetioHttp,
    args: &NetioHttpConfig,
) -> Result<Vec<f64>, SensorError> {
    let start = Instant::now();
    let client = Client::new();

    let tasks = args.pdus.iter().map(|pdu| {
        let c = client.clone();
        async move {
            let res: NetioHttpResponse = c
                .get(&pdu.url)
                .send()
                .await
                .context("Send request")
                .map_err(SensorError::MajorFailure)?
                .json()
                .await
                .context("Parse JSON")
                .map_err(SensorError::MajorFailure)?;
            Ok::<_, SensorError>(res)
        }
    });

    let res = try_join_all(tasks).await?;
    let mut data = Vec::with_capacity(s.0);
    for (res, pdu) in res.into_iter().zip(args.pdus.iter()) {
        data.push(res.global_measure.voltage);
        for load in pdu.loads.iter() {
            let output = res.outputs.iter().find(|x| x.name.eq(load));
            if output.is_none() {
                return Err(SensorError::MajorFailure(eyre!(
                    "Output named {} not found",
                    load
                )));
            }
            let output = output.unwrap();
            data.push(output.current);
            data.push(output.load);
        }
    }

    sleep(Duration::from_millis(min(
        500 - start.elapsed().as_millis() as u64,
        500,
    )))
    .await;
    Ok(data)
}
