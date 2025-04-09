import os
import glob
import common
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plotter(name, bw_log, left_axis, filepath, label_left):
    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(left_axis[0], left_axis[1], label=label_left)
    ax1.set_xlabel("Time (seconds)")
    ax1.set_ylabel(label_left)
    ax1.tick_params(axis="y")
    ax1.set_xlim(left=0)
    ax1.set_ylim(bottom=0)

    ax2 = ax1.twinx()
    ax2.plot(bw_log["time_sec"], bw_log["smoothed"], color="tab:red", label="Bandwidth (MB/s)")
    ax2.set_ylabel("Bandwidth (MB/s)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")
    ax2.set_ylim(bottom=0)

    fig.legend()
    plt.title(f"Bandwidth over time for {name}")
    plt.tight_layout()
    plt.savefig(filepath, format="pdf")
    plt.close()

def trim_to_bw(df, bw_duration):
    extra_time = df["time_sec"].max() - bw_duration
    if extra_time > 0:
        df = df[df["time_sec"] >= extra_time].reset_index(drop=True)
        df["time_sec"] = df["time_sec"] - extra_time
    return df

def gen_plots(plot_dir, results_dir, name):
    ps3 = pd.read_csv(os.path.join(results_dir, "powersensor3.csv"), dtype='float32')
    rapl = pd.read_csv(os.path.join(results_dir, "pmt-RAPL.csv"), dtype='float32')
    sysinfo = pd.read_csv(os.path.join(results_dir, "sysinfo.csv"), dtype='float32')
    log_files = sorted(glob.glob(os.path.join(results_dir, "log_bw.*.log")))

    bw_logs = []
    for file in log_files:
        df = pd.read_csv(file, names=["time", "bw", "direction", "offset", "unknown"], dtype='float32')
        df['bw'] = df['bw'] / 1024
        bw_logs.append(df)

    ps3.dropna(inplace=True)
    rapl.dropna(inplace=True)
    sysinfo.dropna(inplace=True)

    point = len(ps3)
    for df in bw_logs:
        df["time_sec"] = df.index * (point / (point * 10))

    bw_dfs = []
    for df in bw_logs:
        temp_df = df[['time_sec', 'bw']].copy()
        temp_df.set_index('time_sec', inplace=True)
        bw_dfs.append(temp_df)

    combined = pd.concat(bw_dfs, axis=1)
    summed_bw = combined.sum(axis=1).dropna().reset_index()
    summed_bw.columns = ['time_sec', 'bw']
    bw_log = summed_bw.copy()

    rapl = rapl[(rapl["Total"] < 300) & (rapl["Total"] >= 0)].copy()
    sysinfo["average_freq_node0"] = sysinfo[[f"cpu-{i}-freq" for i in range(10)]].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo[[f"cpu-{i}-freq" for i in range(10, 20)]].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo[[f"cpu-{i}-load" for i in range(10)]].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo[[f"cpu-{i}-load" for i in range(10, 20)]].mean(axis=1)

    ps3["total_smoothed"] = ps3["Total"].rolling(window=100, center=True).mean()
    rapl["total_smoothed"] = rapl["Total"].rolling(window=100, center=True).mean()
    bw_log["smoothed"] = bw_log["bw"].rolling(window=10, center=True).mean()

    sysinfo["average_freq_node0"] = sysinfo.loc[:, "cpu-0-freq":"cpu-9-freq"].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo.loc[:, "cpu-9-freq":"cpu-19-freq"].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo.loc[:, "cpu-0-load":"cpu-9-load"].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo.loc[:, "cpu-9-load":"cpu-19-load"].mean(axis=1)

    point = len(ps3)
    ps3["time_sec"] = ps3.index * (point / (point * 1000))
    rapl["time_sec"] = rapl.index * (point / (point * 1000))
    sysinfo["time_sec"] = sysinfo.index * (point / (point * 10))

    bw_duration = bw_log["time_sec"].max()
    ps3 = trim_to_bw(ps3, bw_duration)
    rapl = trim_to_bw(rapl, bw_duration)
    sysinfo = trim_to_bw(sysinfo, bw_duration)

    ps3.reset_index(drop=True, inplace=True)
    rapl.reset_index(drop=True, inplace=True)
    sysinfo.reset_index(drop=True, inplace=True)
    bw_log.reset_index(drop=True, inplace=True)

    plotter(name, bw_log, [ps3["time_sec"], ps3["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-ssd-power.pdf"), "SSD power (Watts)")
    plotter(name, bw_log, [rapl["time_sec"], rapl["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-power.pdf"), "CPU + DRAM power (Watts)")
    plotter(name, bw_log, [sysinfo["time_sec"], sysinfo["average_freq_node0"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-freq-0.pdf"), label_left="CPU frequency (MHz)")
    plotter(name, bw_log, [sysinfo["time_sec"], sysinfo["average_load_node0"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-load-0.pdf"), label_left="CPU load")
    plotter(name, bw_log, [sysinfo["time_sec"], sysinfo["average_freq_node1"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-freq-1.pdf"), label_left="CPU frequency (MHz)")
    plotter(name, bw_log, [sysinfo["time_sec"], sysinfo["average_load_node1"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-load-1.pdf"), label_left="CPU load")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot_dir", type=str, required=True)
    parser.add_argument("--results_dir", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)
    args = parser.parse_args()

    gen_plots(args.plot_dir, args.results_dir, args.name)
