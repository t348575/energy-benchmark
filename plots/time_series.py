import os
import glob
import argparse
import json
import yaml
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List

import pandas as pd
from scipy.signal import savgol_filter
import matplotlib.pyplot as plt

import common

def clean_sensor(sensor: str, spec: "Spec", df: pd.DataFrame) -> pd.DataFrame:
    match sensor:
        case "diskstat.csv":
            return common.fill_clean(df, trim=spec.trim_from_end, offset=spec.offset, fillmode="spread", fillmodespread=10)
        case "netio-http.csv":
            return common.fill_clean(df, trim=spec.trim_from_end, offset=spec.offset, fillmode="spread", fillmodespread=500)
        case _:
            return common.fill_clean(df, trim=spec.trim_from_end, offset=spec.offset)

def prepare_sensor(sensor: str, spec: "Spec", bench_config, bench_info, df: pd.DataFrame) -> pd.DataFrame:
    if spec.trim_from_end < 101:
        window_length = len(spec.trim_from_end) - 1
    else:
        window_length = 101
    match sensor:
        case "sysinfo.csv":
            for numa_domain in bench_info["cpu_topology"]:
                cores = bench_info["cpu_topology"][numa_domain]
                numa_domain = int(numa_domain)
                start = numa_domain * cores
                df[f"average_freq_node{numa_domain}"] = df.loc[:, f"cpu-{start}-freq":f"cpu-{start + cores - 1}-freq"].max(axis=1)
                df[f"average_load_node{numa_domain}"] = df.loc[:, f"cpu-{start}-load":f"cpu-{start + cores - 1}-load"].mean(axis=1)
        case "powersensor3.csv":
            df["total_smoothed"] = savgol_filter(df["Total"], window_length=window_length, polyorder=3)
        case "rapl.csv":
            df = df[(df["Total"] < bench_config["settings"]["cpu_max_power_watts"]) & (df["Total"] >= 0)].copy()
            df["total_smoothed"] = savgol_filter(df["Total"], window_length=window_length, polyorder=3)
        case "diskstat.csv":
            df["read"] = df["read"] / 1048576
            df["write"] = df["write"] / 1048576
            df["total"] = df["read"] + df["write"]
            df["total_smoothed"] = savgol_filter(df["total"], window_length=window_length, polyorder=3)
            df["read_smoothed"] = savgol_filter(df["read"], window_length=window_length, polyorder=3)
            df["write_smoothed"] = savgol_filter(df["write"], window_length=window_length, polyorder=3)
    return df

def read_prepare_sensor_data(spec: "Spec", bench_config, bench_info, bench_data):
    sensors = {}
    for sensor in spec.sensors:
        df = pd.read_csv(os.path.join(spec.results_dir, sensor), dtype="float32")
        df.dropna(inplace=True)
        if sensor == "powersensor3.csv" and spec.bench_type != "fio":
            spec.trim_from_end = len(df) - spec.trim_end
        df = clean_sensor(sensor, spec, df)
        df = prepare_sensor(sensor, spec, bench_config, bench_info, df)
        # if sensor == "rapl.csv" and spec.bench_type == "fio":
            # df = df[df["time"] > bench_data["offset"]]
            # df["dt"] = df["time"].diff()

            # weighted_sum = (df["Total"] * df["dt"]).sum()
            # total_time = df["time"].iloc[-1] - df["time"].iloc[0]
            # print(weighted_sum / total_time)
        sensors[sensor] = df
    return sensors

nvme_trace_agg_options = {
    'count': 'sum',
    'function': 'first',
    'is_nvme_call': 'first',
    'has_fs_pagewrite': 'first',
    'requeued_io': 'first',
    'vfs_read': 'first',
    'vfs_write': 'first',
    'vfs_fsync': 'first'
}

def build_trace_graphs(trace_file, orig_rows):
    trace_graphs = []
    trace_data = pd.read_csv(trace_file)

    trace_data_all = (
        trace_data.groupby(["time"], as_index=False).agg(nvme_trace_agg_options)
    )
    trace_data_all = common.offset_trace_time(trace_data_all, spec.results_dir)

    fcol = trace_data_all.get("function")
    has_read_func = False
    has_write_func = False
    if fcol is not None:
        has_read_func = (fcol == "read+76").any()
        has_write_func = (fcol == "__write+79").any()

    if has_read_func:
        trace_data_read = trace_data_all.loc[trace_data_all["function"] == "read+76"]
        trace_data_read = common.fill_clean(
            trace_data_read, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
        )
        if not trace_data_read.empty:
            trace_graphs.append([trace_data_read["time"], trace_data_read["count"], "read I/O"])
    else:
        vfs_read = trace_data.get("vfs_read", pd.Series(False, index=trace_data.index))
        if vfs_read.any():
            tmp = (
                trace_data.loc[vfs_read]
                .groupby(["time"], as_index=False)
                .agg(nvme_trace_agg_options)
            )
            tmp = common.offset_trace_time(tmp, spec.results_dir)
            if "is_nvme_call" in tmp.columns:
                tmp = tmp.loc[tmp["is_nvme_call"] == True]
            if not tmp.empty:
                trace_data_read = common.fill_clean(
                    tmp, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
                )
                trace_graphs.append([trace_data_read["time"], trace_data_read["count"], "read I/O"])

    trace_data_write = None
    if has_write_func:
        trace_data_write = trace_data_all.loc[trace_data_all["function"] == "__write+79"]
        trace_data_write = common.fill_clean(
            trace_data_write, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
        )
    else:
        vfs_write = trace_data.get("vfs_write", pd.Series(False, index=trace_data.index))
        is_nvme_call = trace_data.get("is_nvme_call", pd.Series(False, index=trace_data.index))
        mask = vfs_write & is_nvme_call
        if mask.any():
            tmp = (
                trace_data.loc[vfs_write]
                .groupby(["time"], as_index=False)
                .agg(nvme_trace_agg_options)
            )
            tmp = common.offset_trace_time(tmp, spec.results_dir)
            if "is_nvme_call" in tmp.columns:
                tmp = tmp.loc[tmp["is_nvme_call"] == True]
            if not tmp.empty:
                trace_data_write = common.fill_clean(
                    tmp, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
                )

    if trace_data_write is not None and not trace_data_write.empty:
        trace_graphs.append([trace_data_write["time"], trace_data_write["count"], "write I/O"])

    def _agg_flag(flag_col: str) -> pd.DataFrame:
        flag = trace_data.get(flag_col, pd.Series(False, index=trace_data.index))
        if not flag.any():
            return pd.DataFrame()
        tmp = (
            trace_data.loc[flag]
            .groupby(["time"], as_index=False)
            .agg(nvme_trace_agg_options)
        )
        tmp = common.offset_trace_time(tmp, spec.results_dir)
        return tmp

    # writepage
    trace_data_fs_writepage = _agg_flag("has_fs_pagewrite")
    if not trace_data_fs_writepage.empty:
        trace_data_fs_writepage = common.fill_clean(
            trace_data_fs_writepage, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
        )
        if not trace_data_fs_writepage.empty:
            trace_graphs.append([trace_data_fs_writepage["time"], trace_data_fs_writepage["count"], "write page file"])

    trace_data_requeued_io = _agg_flag("requeued_io")
    if not trace_data_requeued_io.empty:
        if "is_nvme_call" in trace_data_requeued_io.columns:
            trace_data_requeued_io = trace_data_requeued_io.loc[trace_data_requeued_io["is_nvme_call"] == True]
        if not trace_data_requeued_io.empty:
            trace_data_requeued_io = common.fill_clean(
                trace_data_requeued_io, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
            )
            if not trace_data_requeued_io.empty:
                trace_graphs.append([trace_data_requeued_io["time"], trace_data_requeued_io["count"], "requeue I/O"])

    trace_data_vfs_fsync = _agg_flag("vfs_fsync")
    if not trace_data_vfs_fsync.empty:
        if "is_nvme_call" in trace_data_vfs_fsync.columns:
            trace_data_vfs_fsync = trace_data_vfs_fsync.loc[trace_data_vfs_fsync["is_nvme_call"] == True]
        if not trace_data_vfs_fsync.empty:
            trace_data_vfs_fsync = common.fill_clean(
                trace_data_vfs_fsync, fillmode="spread", fillmodespread=1000, offset=spec.offset, trim=spec.trim_from_end
            )
            if not trace_data_vfs_fsync.empty:
                trace_graphs.append([trace_data_vfs_fsync["time"], trace_data_vfs_fsync["count"], "fsync"])

    return trace_graphs

def read_prepare_bench_data(spec: "Spec"):
    results = {}
    marker_file = os.path.join(spec.results_dir, "markers.csv")
    if os.path.exists(marker_file):
        results["markers"] = pd.read_csv(marker_file)

    match spec.bench_type:
        case "fio":
            log_files = sorted(glob.glob(os.path.join(spec.results_dir, "log_bw.*.log")))
            bw_logs = []
            for file in log_files:
                df = pd.read_csv(file, names=["time", "bw", "direction", "offset", "unknown"], dtype="float32").groupby("time", as_index=False)["bw"].sum()
                df["bw"] = df["bw"] / 1024
                bw_logs.append(df)
            bw_dfs = []
            for df in bw_logs:
                temp_df = df[["time", "bw"]].copy()
                temp_df.set_index("time", inplace=True)
                bw_dfs.append(temp_df)

            sum_bw = pd.concat([df["bw"].reset_index(drop=True) for df in bw_dfs], axis=1).sum(axis=1)
            time = bw_dfs[0].index.to_series().reset_index(drop=True)
            bw_log = pd.DataFrame({"time": time, "bw": sum_bw})
            bw_log = bw_log.dropna()

            f = open(os.path.join(spec.results_dir, "results.json"))
            results = json.load(f)
            if "ramp_time" in results["jobs"][0]["job options"]:
                ramp_time = int(common.parse_time_string(results["jobs"][0]["job options"]) / 1000)
            elif "global options" in results and "ramp_time" in results["global options"]:
                ramp_time = int(common.parse_time_string(results["global options"]["ramp_time"]) / 1000)
            else:
                ramp_time = 0

            ramp_time = max(0, ramp_time - spec.offset)
            ps3_df = pd.read_csv(os.path.join(spec.results_dir, "powersensor3.csv"), dtype="float32")
            ps3_df.dropna(inplace=True)
            orig_rows = len(ps3_df)
            spec.trim_from_end = orig_rows - spec.trim_end
            bw_log = common.fill_clean(bw_log, offset=spec.offset, trim=spec.trim_from_end)
            bw_log["smoothed"] = savgol_filter(bw_log["bw"], window_length=101, polyorder=3)
            results["data"] = bw_log
            results["offset"] = ramp_time
    return results

@dataclass
class Axis:
    axis_type: str
    dataset_name: str
    dataset_field: str
    plot_label: str
    axis_label: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Axis":
        axis_type = data.get("axis_type")
        if axis_type is None:
            raise ValueError("Axis definition requires '_type'")
        return cls(
            axis_type=axis_type,
            dataset_name=data["dataset_name"],
            dataset_field=data["dataset_field"],
            plot_label=data["plot_label"],
            axis_label=data["axis_label"],
        )

    def fetch_data(self, sensors: Dict[str, pd.DataFrame], bench_data: Optional[Any], skip_offset=False):
        if "offset" in bench_data and self.dataset_field == "time":
            offset = bench_data["offset"]
        else:
            offset = 0
        if skip_offset:
            offset = 0
        if self.axis_type == "sensor":
            return sensors[self.dataset_name][self.dataset_field] + offset
        if self.axis_type == "bench":
            return bench_data["data"][self.dataset_field] + offset
        raise ValueError(f"Unsupported axis type '{self.axis_type}'")

    def copy_as_time(self):
        return Axis(
            axis_type=self.axis_type,
            dataset_name=self.dataset_name,
            dataset_field="time",
            plot_label=self.plot_label,
            axis_label=self.axis_label,
        )

@dataclass
class Plot:
    y_axis: List[Axis]
    time: Axis
    secondary_y_axis: List[Axis] = field(default_factory=list)
    title: str = ""
    file_name: str = ""
    dir: str = ""
    x_axis: Optional[Axis] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Plot":
        return cls(
            y_axis=[Axis.from_dict(item) for item in data.get("y_axis", [])],
            time=Axis.from_dict(data["time"]),
            secondary_y_axis=[Axis.from_dict(item) for item in data.get("secondary_y_axis", [])],
            title=data.get("title", ""),
            file_name=data.get("file_name", ""),
            dir=data.get("dir", ""),
            x_axis=Axis.from_dict(data["x_axis"]) if data.get("x_axis") else None,
        )


@dataclass
class Spec:
    plot_dir: Optional[str] = None
    results_dir: Optional[str] = None
    name: Optional[str] = None
    bench_type: Optional[str] = None
    config_yaml: Optional[str] = None
    info_json: Optional[str] = None
    plots: List[Plot] = field(default_factory=list)

    offset: Optional[int] = 0
    trim_end: Optional[int] = 0
    width: Optional[int] = 12
    trim_from_end: Optional[int] = None

    sensors: List[str] = field(default_factory=list)

    @classmethod
    def from_json_file(cls, s: Optional[str]) -> "Spec":
        if not s:
            return cls()
        with open(s, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        plots = [Plot.from_dict(item) for item in data.get("plots", [])]
        if "sensors" in data:
            sensors = data["sensors"]
        else:
            sensors = ["powersensor3.csv", "rapl.csv", "sysinfo.csv", "diskstat.csv"]
        return cls(
            plot_dir=data.get("plot_dir"),
            results_dir=data.get("results_dir"),
            name=data.get("name"),
            bench_type=data.get("bench_type"),
            config_yaml=data.get("config_yaml"),
            info_json=data.get("info_json"),
            offset=data.get("offset", 0),
            trim_end=data.get("trim_end", 0),
            width=data.get("width", 12),
            plots=plots,
            sensors=sensors,
        )

    @classmethod
    def from_args(cls, args: argparse.Namespace) -> "Spec":
        return cls(
            plot_dir=args.plot_dir,
            results_dir=args.results_dir,
            name=args.name,
            bench_type=args.bench_type,
            offset=args.offset,
            trim_end=args.trim_end,
            width=args.width,
        )

    def merge_overrides(self, overrides: Dict[str, Any]) -> "Spec":
        for k, v in overrides.items():
            if k == "plots" or k == "sensors" or v is None:
                continue
            setattr(self, k, v)
        return self

    def validate(self):
        missing = [k for k in ("plot_dir", "results_dir", "name") if getattr(self, k) in (None, "")]
        if missing:
            raise Exception("Must specify either --spec or --plot_dir, --results_dir, --name")

def plot(p: "Plot", spec: "Spec", sensors: Dict[str, pd.DataFrame], bench_data, bench_config, bench_info):
    color_idx = 0
    fig, ax = plt.subplots(figsize=(spec.width, 6.5))
    for y_axis in p.y_axis:
        ax.plot(p.time.fetch_data(sensors, bench_data), y_axis.fetch_data(sensors, bench_data), color=common.colors[color_idx % len(common.colors)], label=y_axis.plot_label,)
        color_idx += 1

    ax.set_ylabel(p.y_axis[0].axis_label)
    ax.tick_params(axis="y")
    ax.set_ylim(bottom=0)
    ax.set_xlabel(p.time.axis_label)

    if "markers" in bench_data:
        for idx, row in bench_data["markers"].iterrows():
            if row["time"] - spec.offset > 0 and row["time"] < spec.trim_from_end:
                ax.axvline((row["time"] - spec.offset) / 1000, color=common.colors[color_idx], label=row["marker_name"], linestyle="dashdot")
                color_idx += 1

    if p.secondary_y_axis:
        ax2 = ax.twinx()
        for y_axis in p.secondary_y_axis:
            ax2.plot(y_axis.copy_as_time().fetch_data(sensors, bench_data, skip_offset=True), y_axis.fetch_data(sensors, bench_data), color=common.colors[color_idx % len(common.colors)], label=y_axis.plot_label)
            color_idx += 1
        ax2.set_ylabel(p.secondary_y_axis[0].axis_label)
        ax2.tick_params(axis="y")
        ax2.set_xlim(left=0)
        ax2.set_ylim(bottom=0)

    if len(p.y_axis) + len(p.secondary_y_axis) > 1:
        fig.legend(loc="upper right")

    plt.title(p.title)
    plt.tight_layout()
    ymin, ymax = plt.ylim()
    plt.ylim(ymin, ymax * 1.02) 
    plt.savefig(os.path.join(spec.plot_dir, p.dir, f"{p.file_name}.pdf"), format="pdf")
    plt.close()

def calculate_energy(df, time="time", power="Total"):
    dt = df[time].diff().iloc[1:]
    avg_power = (df[power][:-1].values + df[power][1:].values) / 2
    return (avg_power * dt).sum()

def gather_stats(sensors: Dict[str, pd.DataFrame], bench_data, bench_config, bench_info):
    stats = {}
    for sensor in sensors:
        df = sensors[sensor]

        dfs = {}
        match spec.bench_type:
            case "ycsb":
                benchmark = df[df["time"] < bench_data["markers"].iloc[0]["time"] / 1000]
                unmount = df[(df["time"] > bench_data["markers"].iloc[0]["time"] / 1000) & (df["time"] < bench_data["markers"].iloc[1]["time"] / 1000)]
                dfs["total"] = df
                dfs["benchmark"] = benchmark
                dfs["unmount"] = unmount

                stats["times"] = {
                    "benchmark": bench_data["markers"].iloc[0]["time"] / 1000,
                    "unmount": (bench_data["markers"].iloc[1]["time"] - bench_data["markers"].iloc[0]["time"]) / 1000
                }
            case "filebench":
                fileset = df[df["time"] < bench_data["markers"].iloc[0]["time"] / 1000]
                benchmark = df[(df["time"] > bench_data["markers"].iloc[0]["time"] / 1000) & (df["time"] < bench_data["markers"].iloc[1]["time"] / 1000)]
                post_benchmark = df[(df["time"] > bench_data["markers"].iloc[1]["time"] / 1000) & (df["time"] < bench_data["markers"].iloc[2]["time"] / 1000)]

                dfs["fileset"] = fileset
                dfs["benchmark"] = benchmark
                dfs["post_benchmark"] = post_benchmark

                stats["times"] = {
                    "fileset": bench_data["markers"].iloc[0]["time"] / 1000,
                    "benchmark": (bench_data["markers"].iloc[1]["time"] - bench_data["markers"].iloc[0]["time"]) / 1000,
                    "post_benchmark": (bench_data["markers"].iloc[2]["time"] - bench_data["markers"].iloc[1]["time"]) / 1000
                }
            case _:
                dfs["total"] = df

        for set_name in dfs:
            df = dfs[set_name]

            if set_name not in stats:
                stats[set_name] = {}

            match sensor:
                case "powersensor3.csv":
                    stats[set_name]["ssd"] = {
                        "power": df["Total"].mean().item(),
                        "energy": calculate_energy(df),
                    }
                case "rapl.csv":
                    stats[set_name]["cpu"] = {
                        "power": df["Total"].mean().item(),
                        "energy": calculate_energy(df),
                    }
                case "sysinfo.csv":
                    if "cpu" not in stats[set_name]:
                        stats[set_name]["cpu"] = {}
                    for numa_domain in bench_info["cpu_topology"]:
                        stats[set_name]["cpu"][f"average_freq_node{numa_domain}"] = df[f"average_freq_node{numa_domain}"].max().item()
                        stats[set_name]["cpu"][f"average_load_node{numa_domain}"] = df[f"average_load_node{numa_domain}"].mean().item()
                case "diskstat.csv":
                    ps3 = sensors["powersensor3.csv"]
                    read_nonzero = df["read"][df["read"] > 0]
                    write_nonzero = df["write"][df["write"] > 0]
                    active_times = df[(df["read"] > 0) | (df["write"] > 0)]["time"]
                    filtered_energy_df = ps3[ps3["time"].isin(active_times)]

                    if "diskstat" not in stats[set_name]:
                        stats[set_name]["diskstat"] = {}

                    stats[set_name]["diskstat"]["read"] = read_nonzero.mean().item() if not read_nonzero.empty else 0,
                    stats[set_name]["diskstat"]["write"] = write_nonzero.mean().item() if not write_nonzero.empty else 0,
                    if len(filtered_energy_df) > 0:
                        stats[set_name]["ssd"]["io_power"] = filtered_energy_df["Total"].mean().item(),
                    else:
                        stats[set_name]["ssd"]["io_power"] = -1

                    if "extra" in bench_config and "ssd_idle" in bench_config["extra"]:
                        idle = bench_config["extra"]["ssd_idle"]
                    else:
                        idle = 2
                    stats[set_name]["ssd"]["above_idle"] = ps3[ps3["Total"] > idle]["Total"].mean().item()

    return stats

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", type=str, required=False)
    parser.add_argument("--plot_dir", type=str, required=False)
    parser.add_argument("--results_dir", type=str, required=False)
    parser.add_argument("--name", type=str, required=False)
    parser.add_argument("--offset", type=int, required=False)
    parser.add_argument("--trim_end", type=int, required=False)
    parser.add_argument("--width", type=int, required=False)
    parser.add_argument("--bench_type", type=str, required=False)

    args = parser.parse_args()
    base = Spec.from_json_file(args.spec)

    arg_spec = Spec.from_args(args)
    merged = base.merge_overrides(vars(arg_spec))
    merged.validate()
    spec = merged

    bench_config = yaml.safe_load(open(spec.config_yaml, "r", encoding="utf-8"))
    bench_info = json.load(open(spec.info_json, "r", encoding="utf-8"))
    bench_data = read_prepare_bench_data(spec)
    sensors = read_prepare_sensor_data(spec, bench_config, bench_info, bench_data)
    trace_file = os.path.join(spec.plot_dir, "plot_data", f"{spec.name}.csv")
    if os.path.exists(trace_file):
        bench_data["trace"] = build_trace_graphs(trace_file, len(sensors["powersensor3.csv"]))

    for p in spec.plots:
        plot(p, spec, sensors, bench_data, bench_config, bench_info)

    stats = gather_stats(sensors, bench_data, bench_config, bench_info)
    with open(os.path.join(spec.plot_dir, f"{spec.name}-stats.json"), "w") as f:
        f.write(json.dumps(stats, indent=4))
