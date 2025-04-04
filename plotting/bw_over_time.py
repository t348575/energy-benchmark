import os
import common
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plotter(bw_log, left_axis, filepath, color_left, label_left):
    _, ax1 = plt.subplots(figsize=(12, 5))
    ax1.plot(left_axis[0], left_axis[1], color=color_left, label=label_left)
    ax1.set_xlabel("Time (seconds)")
    ax1.set_ylabel(label_left, color=color_left)
    ax1.tick_params(axis="y", labelcolor=color_left)
    # ax1.set_xlim(left=0,right=0)

    ax2 = ax1.twinx()
    ax2.plot(bw_log["time_sec"], bw_log["smoothed"], color="tab:red", label="Bandwidth (MB/s)")
    ax2.set_ylabel("Bandwidth (MB/s)", color="tab:red")
    ax2.tick_params(axis="y", labelcolor="tab:red")

    plt.tight_layout()
    plt.savefig(filepath, format="pdf")
    plt.close()

def gen_plots(plot_dir, results_dir, name):
    ps3 = pd.read_csv(os.path.join(results_dir, "powersensor3.csv"))
    rapl = pd.read_csv(os.path.join(results_dir, "pmt-RAPL.csv"))
    sysinfo = pd.read_csv(os.path.join(results_dir, "sysinfo.csv"))
    bw_log = pd.read_csv(os.path.join(results_dir, "log_bw.1.log"), names=["time", "bw", "direction", "offset", "unknown"])

    ps3.dropna(inplace=True)
    rapl.dropna(inplace=True)
    bw_log.dropna(inplace=True)
    sysinfo.dropna(inplace=True)

    bw_log["bw"] = bw_log["bw"] / 1024

    rapl = rapl[rapl["Total"] < 300].reset_index(drop=True)
    rapl = rapl[(rapl >= 0).all(axis=1)].reset_index(drop=True)
    bw_log = bw_log[bw_log["time"] > 5000].reset_index(drop=True)
    sysinfo["average_freq_node0"] = sysinfo.loc[:, "cpu-0-freq":"cpu-9-freq"].max(axis=1)
    sysinfo["average_freq_node1"] = sysinfo.loc[:, "cpu-9-freq":"cpu-19-freq"].max(axis=1)

    sysinfo["average_load_node0"] = sysinfo.loc[:, "cpu-0-load":"cpu-9-load"].mean(axis=1)
    sysinfo["average_load_node1"] = sysinfo.loc[:, "cpu-9-load":"cpu-19-load"].mean(axis=1)

    ps3["total_smoothed"] = ps3["Total"].rolling(window=100, center=True).mean().reset_index(drop=True)
    rapl["total_smoothed"] = rapl["Total"].rolling(window=100, center=True).mean().reset_index(drop=True)
    bw_log["smoothed"] = bw_log["bw"].rolling(window=100, center=True).mean().reset_index(drop=True)

    sysinfo["average_freq_node0_smoothed"] = sysinfo["average_freq_node0"].rolling(window=100, center=True).mean().reset_index(drop=True)
    sysinfo["average_freq_node1_smoothed"] = sysinfo["average_freq_node1"].rolling(window=100, center=True).mean().reset_index(drop=True)
    sysinfo["average_load_node0_smoothed"] = sysinfo["average_load_node0"].rolling(window=100, center=True).mean().reset_index(drop=True)
    sysinfo["average_load_node1_smoothed"] = sysinfo["average_load_node1"].rolling(window=100, center=True).mean().reset_index(drop=True)

    ps3["time_sec"] = ps3.index * 0.001
    rapl["time_sec"] = rapl.index * 0.001
    sysinfo["time_sec"] = np.arange(len(sysinfo)) * 0.25
    bw_log["time_sec"] = bw_log.index * 0.01

    ps3.reset_index(drop=True, inplace=True)
    rapl.reset_index(drop=True, inplace=True)
    sysinfo.reset_index(drop=True, inplace=True)
    bw_log.reset_index(drop=True, inplace=True)

    plotter(bw_log, [ps3["time_sec"], ps3["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-ssd-power.pdf"), "tab:blue", "SSD power (Watts)")
    plotter(bw_log, [rapl["time_sec"], rapl["total_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-power.pdf"), "tab:green", "CPU + DRAM power (Watts)")
    plotter(bw_log, [sysinfo["time_sec"], sysinfo["average_freq_node0_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-freq-0.pdf"), "tab:orange", "CPU frequency (MHz)")
    plotter(bw_log, [sysinfo["time_sec"], sysinfo["average_load_node0_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-load-0.pdf"), "tab:purple", "CPU load")
    plotter(bw_log, [sysinfo["time_sec"], sysinfo["average_freq_node1_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-freq-1.pdf"), "tab:orange", "CPU frequency (MHz)")
    plotter(bw_log, [sysinfo["time_sec"], sysinfo["average_load_node1_smoothed"]], os.path.join(plot_dir, f"{name}-bw-vs-cpu-load-1.pdf"), "tab:purple", "CPU load")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--plot_dir", type=str, required=True)
    parser.add_argument("--results_dir", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)
    args = parser.parse_args()

    gen_plots(args.plot_dir, args.results_dir, args.name)
