use serde::{Deserialize, Serialize};

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TpccPostgresMetrics {
    pub summary: Summary,
}

#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Summary {
    pub name: String,
    pub time_seconds: i64,
    pub measure_start_ts: i64,
    pub warehouses: i64,
    pub new_orders: i64,
    pub tpmc: i64,
    pub efficiency: f64,
    pub throughput: i64,
    pub goodput: i64,
    pub completed_new_orders: i64,
    pub completed_paymens: i64,
}
