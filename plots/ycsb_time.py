import os
import glob
import json
import common
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plotter(data, marker_data, filepath, label, offset, trim, width=256, double_axis=None, label_left=None, title=None):
    fig, ax = plt.subplots(figsize=(width, 6.5))
    ax.plot(data[0], data[1], color=common.colors[0], label="Disk Throughput")
    ax.tick_params(axis="y")
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Time (s)")
    ax.set_ylabel(label)

    if marker_data is not None:
        for idx, row in marker_data.iterrows():
            if row["time"] - offset > 0 and row["time"] < trim:
                ax.axvline((row["time"] - offset) / 1000, color=common.colors[idx + 1], label=row["marker_name"], linestyle="dashdot")

    if double_axis is not None:
        ax2 = ax.twinx()

        ax2.plot(double_axis[0], double_axis[1], color=common.colors[1], label="SSD Power")
        ax2.set_ylabel(label_left)
        ax2.tick_params(axis="y")
        ax2.set_xlim(left=0)
        ax2.set_ylim(bottom=0)

    plt.title(title if title is not None else f"YCSB")
    plt.tight_layout()
    fig.legend(bbox_to_anchor=(0.96, 0.94), loc="upper right")
    plt.savefig(filepath, format="pdf")
    plt.close()

def plot_trace(data, left_axis, marker_data, filepath, label, label_left, offset, trim, title, width=256):
    fig, ax1 = plt.subplots(figsize=(width, 6.5))
    ax1.plot(data[0], data[1], color=common.colors[0], label=label)
    ax1.set_ylabel(label)
    ax1.tick_params(axis="y")
    ax1.set_ylim(bottom=0)
    ax1.set_xlabel("Time (s)")

    idx = 0
    if marker_data is not None:
        for idx, row in marker_data.iterrows():
            if row["time"] - offset > 0 and row["time"] < trim:
                ax1.axvline((row["time"] - offset) / 1000, color=common.colors[idx + 1], label=row["marker_name"], linestyle="dashdot")

    ax2 = ax1.twinx()
    for i, item in enumerate(left_axis):
        ax2.plot(item[0], item[1], color=common.colors[(idx + 3 + i) % len(common.colors)], label=f"{item[2]} calls")
    ax2.set_ylabel(label_left)
    ax2.tick_params(axis="y")
    ax2.set_xlim(left=0)
    ax2.set_ylim(bottom=0)

    fig.legend()
    plt.title(title)
    plt.tight_layout()
    plt.savefig(filepath, format="pdf")
    plt.close()

def calculate_energy(df, time="time", power="Total"):
    dt = df[time].diff().iloc[1:]
    avg_power = (df[power][:-1].values + df[power][1:].values) / 2
    return (avg_power * dt).sum()

def gen_plots(plot_dir, results_dir, name, offset=0, trim=0, width=256):
    ps3 = pd.read_csv(os.path.join(results_dir, "powersensor3.csv"), dtype="float32")
    rapl = pd.read_csv(os.path.join(results_dir, "rapl.csv"), dtype="float32")
    sysinfo = pd.read_csv(os.path.join(results_dir, "sysinfo.csv"), dtype="float32")
    diskstat = pd.read_csv(os.path.join(results_dir, "diskstat.csv"), dtype="float32")

    ps3.dropna(inplace=True)
    rapl.dropna(inplace=True)
    sysinfo.dropna(inplace=True)
    orig_rows = len(ps3)

    marker_file = os.path.join(results_dir, "markers.csv")
    marker_data = None
    if os.path.exists(marker_file):
        marker_data = pd.read_csv(marker_file)

    trace_file = os.path.join(plot_dir, "plot_data", f"{name}.csv")
    trace_data_all = None
    trace_graphs = []
    trim_from_end = orig_rows - trim
    if os.path.exists(trace_file):
        trace_data = pd.read_csv(trace_file)
        trace_data_all = trace_data.groupby(["time"], as_index=False).agg(common.nvme_trace_agg_options)
        trace_data_all = common.offset_trace_time(trace_data_all, results_dir)

        if len(trace_data_all[trace_data_all["function"] == "read+76"]) > 0:
            trace_data_read = common.fill_clean(trace_data_all[trace_data_all["function"] == "read+76"], fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)
            if len (trace_data_read) > 0:
                trace_graphs.append([trace_data_read["time"], trace_data_read["count"], "read I/O"])
        elif len(trace_data[trace_data["vfs_read"] == True]) > 0:
            trace_data_read = trace_data[trace_data["vfs_read"] == True].groupby(["time"], as_index=False).agg(common.nvme_trace_agg_options)
            trace_data_read = trace_data_read[trace_data_read["is_nvme_call"] == True]
            if len(trace_data_read) > 0:
                trace_data_read = common.offset_trace_time(trace_data_read, results_dir)
                trace_data_read = common.fill_clean(trace_data_read, fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)
                trace_graphs.append([trace_data_read["time"], trace_data_read["count"], "read I/O"])

        trace_data_write = None
        if len(trace_data_all[trace_data_all["function"] == "__write+79"]) > 0:
            trace_data_write = common.fill_clean(trace_data_all[trace_data_all["function"] == "__write+79"], fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)
        elif len(trace_data[(trace_data["vfs_write"] == True) & (trace_data["is_nvme_call"] == True)]) > 0:
            trace_data_write = trace_data[trace_data["vfs_write"] == True].groupby(["time"], as_index=False).agg(common.nvme_trace_agg_options)
            trace_data_write = common.offset_trace_time(trace_data_write, results_dir)
            trace_data_write = common.fill_clean(trace_data_write[trace_data_write["is_nvme_call"] == True], fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)

        if trace_data_write is not None:
            trace_graphs.append([trace_data_write["time"], trace_data_write["count"], "write I/O"])

        trace_data_fs_writepage = trace_data[trace_data["has_fs_pagewrite"] == True].groupby(["time"], as_index=False).agg(common.nvme_trace_agg_options)
        trace_data_requeued_io = trace_data[trace_data["requeued_io"] == True].groupby(["time"], as_index=False).agg(common.nvme_trace_agg_options)
        trace_data_vfs_fsync = trace_data[trace_data["vfs_fsync"] == True].groupby(["time"], as_index=False).agg(common.nvme_trace_agg_options)

        trace_data_fs_writepage = common.offset_trace_time(trace_data_fs_writepage, results_dir)
        trace_data_fs_writepage = common.fill_clean(trace_data_fs_writepage, fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)
        if len(trace_data_fs_writepage) > 0:
            trace_graphs.append([trace_data_fs_writepage["time"], trace_data_fs_writepage["count"], "write page file"])

        trace_data_requeued_io = common.offset_trace_time(trace_data_requeued_io, results_dir)
        if len(trace_data_requeued_io[trace_data_requeued_io["is_nvme_call"] == True]) > 0:
            trace_data_requeued_io = common.fill_clean(trace_data_requeued_io[trace_data_requeued_io["is_nvme_call"] == True], fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)
            if len(trace_data_requeued_io) > 0:
                trace_graphs.append([trace_data_requeued_io["time"], trace_data_requeued_io["count"], "requeue I/O"])

        if len(trace_data_vfs_fsync[trace_data_vfs_fsync["is_nvme_call"] == True]) > 0:
            trace_data_vfs_fsync = common.offset_trace_time(trace_data_vfs_fsync, results_dir)
            trace_data_vfs_fsync = common.fill_clean(trace_data_vfs_fsync[trace_data_vfs_fsync["is_nvme_call"] == True], fillmode="spread", fillmodespread=1000, offset=offset, trim=trim_from_end)
            if len(trace_data_vfs_fsync) > 0:
                trace_graphs.append([trace_data_vfs_fsync["time"], trace_data_vfs_fsync["count"], "fsync"])


    diskstat["read"] = diskstat["read"] / 1048576
    diskstat["write"] = diskstat["write"] / 1048576
    diskstat["total"] = diskstat["read"] + diskstat["write"]

    ps3 = common.fill_clean(ps3, offset=offset, trim=trim_from_end)
    rapl = common.fill_clean(rapl, offset=offset, trim=trim_from_end)
    sysinfo = common.fill_clean(sysinfo, offset=offset, trim=trim_from_end)
    diskstat = common.fill_clean(diskstat, offset=offset, trim=trim_from_end, fillmode="spread", fillmodespread=10)

    rapl = rapl[(rapl["Total"] < 300) & (rapl["Total"] >= 0)].copy()
    sysinfo["average_freq_node0"] = sysinfo[[f"cpu-{i}-freq" for i in range(10)]].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo[[f"cpu-{i}-freq" for i in range(10, 20)]].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo[[f"cpu-{i}-load" for i in range(10)]].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo[[f"cpu-{i}-load" for i in range(10, 20)]].mean(axis=1)

    from scipy.signal import savgol_filter
    if len(ps3) < 101:
        window_length = len(ps3) - 1
    else:
        window_length = 101
    ps3["total_smoothed"] = savgol_filter(ps3["Total"], window_length=window_length, polyorder=3)
    rapl["total_smoothed"] = savgol_filter(rapl["Total"], window_length=window_length, polyorder=3)

    sysinfo["average_freq_node0"] = sysinfo.loc[:, "cpu-0-freq":"cpu-9-freq"].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo.loc[:, "cpu-9-freq":"cpu-19-freq"].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo.loc[:, "cpu-0-load":"cpu-9-load"].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo.loc[:, "cpu-9-load":"cpu-19-load"].mean(axis=1)

    base = os.path.join(plot_dir, name)
    os.makedirs(base, exist_ok=True)
    if trace_data_all is not None:
        plot_trace([diskstat["time"], diskstat["total"]], trace_graphs, marker_data, os.path.join(base, f"{name}-trace.pdf"),
                    "Total throughput (MiB/s)", "Num. function calls", offset, trim_from_end, "Throughput & function traces vs. Time", width)
    plotter([diskstat["time"], diskstat["total"]], marker_data, os.path.join(base, f"{name}-bw-ssd.pdf"),
                    "Total throughput (MiB/s)", offset, trim_from_end, width, double_axis=[ps3["time"], ps3["Total"]], label_left="Power (Watts)", title="YCSB workload F, 100m records, disk I/O subsection")
    plotter([ps3["time"], ps3["total_smoothed"]], marker_data, os.path.join(base, f"{name}-ssd-power.pdf"), "SSD power (Watts)", offset, trim_from_end, width)
    plotter([rapl["time"], rapl["total_smoothed"]], marker_data, os.path.join(base, f"{name}-cpu-power.pdf"), "CPU power (Watts)", offset, trim_from_end, width)
    plotter([diskstat["time"], diskstat["total"]], marker_data, os.path.join(base, f"{name}-diskstat.pdf"), "Iostat throughput (MiB/s)", offset, trim_from_end, width)
    plotter([diskstat["time"], diskstat["read"]], marker_data, os.path.join(base, f"{name}-read-diskstat.pdf"), "Iostat throughput (MiB/s)", offset, trim_from_end, width)
    plotter([diskstat["time"], diskstat["write"]], marker_data, os.path.join(base, f"{name}-write-diskstat.pdf"), "Iostat throughput (MiB/s)", offset, trim_from_end, width)
    plotter([sysinfo["time"], sysinfo["average_freq_node0"]], marker_data, os.path.join(base, f"{name}-cpu-freq-0.pdf"), "CPU frequency (MHz)", offset, trim_from_end, width)
    plotter([sysinfo["time"], sysinfo["average_load_node0"]], marker_data, os.path.join(base, f"{name}-cpu-load-0.pdf"), "CPU load", offset, trim_from_end, width)
    plotter([sysinfo["time"], sysinfo["average_freq_node1"]], marker_data, os.path.join(base, f"{name}-cpu-freq-1.pdf"), "CPU frequency (MHz)", offset, trim_from_end, width)
    plotter([sysinfo["time"], sysinfo["average_load_node1"]], marker_data, os.path.join(base, f"{name}-cpu-load-1.pdf"), "CPU load", offset, trim_from_end, width)

    benchmark_values = {}
    benchmark_value_items = [
        [ps3, "Total", "ssd_power"],
        [rapl, "Total", "cpu_power"],
        [sysinfo, "average_freq_node0", "cpu_freq_0"],
        [sysinfo, "average_freq_node1", "cpu_freq_1"],
        [sysinfo, "average_load_node0", "cpu_load_0"],
        [sysinfo, "average_load_node1", "cpu_load_1"],
    ]
    for item in benchmark_value_items:
        try:
            benchmark = item[0][item[0]["time"] < marker_data.iloc[0]["time"] / 1000][item[1]].mean().item()
            unmount = item[0][(item[0]["time"] > marker_data.iloc[0]["time"] / 1000) & (item[0]["time"] < marker_data.iloc[1]["time"] / 1000)][item[1]].mean().item()
        except:
            benchmark = item[0][item[1]].mean().item()
            unmount = item[0][item[1]].mean().item()
        benchmark_values[item[2]] = {
            "benchmark": benchmark,
            "unmount": unmount
        }

    benchmark_value_items = [
        [ps3, "ssd_energy"],
        [rapl, "cpu_energy"]
    ]
    for item in benchmark_value_items:
        benchmark = item[0][item[0]["time"] < marker_data.iloc[0]["time"] / 1000]
        unmount = item[0][(item[0]["time"] > marker_data.iloc[0]["time"] / 1000) & (item[0]["time"] < marker_data.iloc[1]["time"] / 1000)]
        benchmark_values[item[1]] = {
            "total": calculate_energy(item[0]),
            "benchmark": calculate_energy(benchmark),
            "unmount": calculate_energy(unmount)
        }

    benchmark_values["times"] = {
        "bencmark": marker_data.iloc[0]["time"] / 1000,
        "unmount": (marker_data.iloc[1]["time"] - marker_data.iloc[0]["time"]) / 1000
    }

    subset_diskstat = diskstat[diskstat["time"] < marker_data.iloc[0]["time"] / 1000]
    read_nonzero = subset_diskstat["read"][subset_diskstat["read"] > 0]
    write_nonzero = subset_diskstat["write"][subset_diskstat["write"] > 0]

    benchmark_values["diskstat"] = {
        "read": read_nonzero.mean().item() if not read_nonzero.empty else 0,
        "write": write_nonzero.mean().item() if not write_nonzero.empty else 0
    }

    with open(os.path.join(base, f"{name}-stats.json"), "w") as f:
        f.write(json.dumps(benchmark_values, indent=4))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot_dir", type=str, required=True)
    parser.add_argument("--results_dir", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument("--offset", type=int, required=False, default=0)
    parser.add_argument("--trim_end", type=int, required=False, default=0)
    parser.add_argument("--width", type=int, required=False, default=256)
    args = parser.parse_args()

    gen_plots(args.plot_dir, args.results_dir, args.name, args.offset, args.trim_end, args.width)
