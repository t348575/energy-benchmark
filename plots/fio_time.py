import os
import glob
import json
import common
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plotter(name, bw_log, left_axis, filepath, label_left):
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(bw_log["time"], bw_log["smoothed"], color=common.colors[0], label="Throughput (MiB/s)")
    ax1.set_ylabel("Throughput (MiB/s)")
    ax1.tick_params(axis="y")
    ax1.set_ylim(bottom=0)

    ax2 = ax1.twinx()

    ax2.plot(left_axis[0], left_axis[1], color=common.colors[1], label=label_left)
    ax1.set_xlabel("Time (s)")
    ax2.set_ylabel(label_left)
    ax2.tick_params(axis="y")
    ax2.set_xlim(left=0)
    ax2.set_ylim(bottom=0)

    plt.legend()
    plt.title(f"Throughput over time")
    plt.tight_layout()
    plt.savefig(filepath, format="pdf")
    plt.close()

def gen_plots(plot_dir, results_dir, name, offset=0, trim_end=0):
    ps3 = pd.read_csv(os.path.join(results_dir, "powersensor3.csv"), dtype="float32")
    rapl = pd.read_csv(os.path.join(results_dir, "rapl.csv"), dtype="float32")
    sysinfo = pd.read_csv(os.path.join(results_dir, "sysinfo.csv"), dtype="float32")
    diskstat = pd.read_csv(os.path.join(results_dir, "diskstat.csv"), dtype="float32")
    log_files = sorted(glob.glob(os.path.join(results_dir, "log_bw.*.log")))

    bw_logs = []
    for file in log_files:
        df = pd.read_csv(file, names=["time", "bw", "direction", "offset", "unknown"], dtype="float32").groupby("time", as_index=False)["bw"].sum()
        df["bw"] = df["bw"] / 1024
        bw_logs.append(df)

    diskstat["read"] = diskstat["read"] / 1048576
    diskstat["write"] = diskstat["write"] / 1048576
    diskstat["total"] = diskstat["read"] + diskstat["write"]

    ps3.dropna(inplace=True)
    rapl.dropna(inplace=True)
    sysinfo.dropna(inplace=True)

    total_ssd_energy_joules = ps3["Total"].sum() * 0.01

    bw_dfs = []
    for df in bw_logs:
        temp_df = df[["time", "bw"]].copy()
        temp_df.set_index("time", inplace=True)
        bw_dfs.append(temp_df)

    combined = pd.concat(bw_dfs, axis=1)
    summed_bw = combined.sum(axis=1).dropna().reset_index()
    summed_bw.columns = ["time", "bw"]
    bw_log = summed_bw.copy()

    bw_log = common.fill_clean(bw_log, offset=offset, trim=trim_end)
    ps3 = common.fill_clean(ps3, trim=trim_end, offset=offset)
    rapl = common.fill_clean(rapl, trim=trim_end, offset=offset)
    sysinfo = common.fill_clean(sysinfo, trim=trim_end, offset=offset)
    diskstat = common.fill_clean(diskstat, trim=trim_end, offset=offset)

    rapl = rapl[(rapl["Total"] < 300) & (rapl["Total"] >= 0)].copy()
    bw_log = bw_log[(bw_log["bw"] < 4500) & (bw_log["bw"] >= 0)].copy()
    sysinfo["average_freq_node0"] = sysinfo[[f"cpu-{i}-freq" for i in range(10)]].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo[[f"cpu-{i}-freq" for i in range(10, 20)]].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo[[f"cpu-{i}-load" for i in range(10)]].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo[[f"cpu-{i}-load" for i in range(10, 20)]].mean(axis=1)

    from scipy.signal import savgol_filter
    bw_log["smoothed"] = savgol_filter(bw_log["bw"], window_length=101, polyorder=3)
    ps3["total_smoothed"] = savgol_filter(ps3["Total"], window_length=101, polyorder=3)
    rapl["total_smoothed"] = savgol_filter(rapl["package-1"] + rapl["dram-1"], window_length=101, polyorder=3)
    diskstat["total_smoothed"] = savgol_filter(diskstat["total"], window_length=101, polyorder=3)
    diskstat["read_smoothed"] = savgol_filter(diskstat["read"], window_length=101, polyorder=3)
    diskstat["write_smoothed"] = savgol_filter(diskstat["write"], window_length=101, polyorder=3)

    sysinfo["average_freq_node0"] = sysinfo.loc[:, "cpu-0-freq":"cpu-9-freq"].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo.loc[:, "cpu-9-freq":"cpu-19-freq"].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo.loc[:, "cpu-0-load":"cpu-9-load"].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo.loc[:, "cpu-9-load":"cpu-19-load"].mean(axis=1)

    plotter(name, bw_log, [diskstat["time"], diskstat["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-diskstat.pdf"), "Iostat throughput (MiB/s)")
    plotter(name, bw_log, [diskstat["time"], diskstat["read_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-read-diskstat.pdf"), "Iostat throughput (MiB/s)")
    plotter(name, bw_log, [diskstat["time"], diskstat["write_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-write-diskstat.pdf"), "Iostat throughput (MiB/s)")
    plotter(name, bw_log, [ps3["time"], ps3["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-ssd-power.pdf"), "SSD power (Watts)")
    plotter(name, bw_log, [rapl["time"], rapl["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-power.pdf"), "CPU power (Watts)")
    plotter(name, bw_log, [sysinfo["time"], sysinfo["average_freq_node0"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-freq-0.pdf"), label_left="CPU frequency (MHz)")
    plotter(name, bw_log, [sysinfo["time"], sysinfo["average_load_node0"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-load-0.pdf"), label_left="CPU load")
    plotter(name, bw_log, [sysinfo["time"], sysinfo["average_freq_node1"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-freq-1.pdf"), label_left="CPU frequency (MHz)")
    plotter(name, bw_log, [sysinfo["time"], sysinfo["average_load_node1"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-load-1.pdf"), label_left="CPU load")

    ps3_mean = ps3["total_smoothed"].mean().item()
    rapl_mean = rapl["total_smoothed"].mean().item()
    freq_0_mean = sysinfo["average_freq_node0"].mean().item()
    freq_1_mean = sysinfo["average_freq_node1"].mean().item()
    load_0_mean = sysinfo["average_load_node0"].mean().item()
    load_1_mean = sysinfo["average_load_node1"].mean().item()
    with open(os.path.join(plot_dir, f"{name}-stats.json"), "w") as f:
        f.write(json.dumps({
            "ssd_mean": ps3_mean,
            "total_ssd_joules": total_ssd_energy_joules.item(),
            "cpu_mean": rapl_mean,
            "freq_0_mean": freq_0_mean,
            "freq_1_mean": freq_1_mean,
            "load_0_mean": load_0_mean,
            "load_1_mean": load_1_mean,
        }, indent=4))

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot_dir", type=str, required=True)
    parser.add_argument("--results_dir", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument("--offset", type=int, required=False, default=0)
    parser.add_argument("--trim_end", type=int, required=False, default=0)
    args = parser.parse_args()

    gen_plots(args.plot_dir, args.results_dir, args.name, args.offset, args.trim_end)
