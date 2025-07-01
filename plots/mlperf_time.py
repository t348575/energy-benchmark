import os
import glob
import json
import common
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

def plotter(data, marker_data, filepath, label, offset, trim):
    fig, ax = plt.subplots(figsize=(256, 6.5))
    ax.plot(data[0], data[1], color=common.colors[0], label=label)
    ax.set_ylabel(label)
    ax.tick_params(axis="y")
    ax.set_ylim(bottom=0)
    ax.set_xlabel("Time (s)")

    if marker_data is not None:
        for idx, row in marker_data.iterrows():
            if row["time"] - offset > 0 and row["time"] < trim:
                ax.axvline((row["time"] - offset) / 1000, color=common.colors[idx + 1], label=row["marker_name"], linestyle="dashdot")

    plt.legend()
    plt.title(f"Time")
    plt.tight_layout()
    plt.savefig(filepath, format="pdf")
    plt.close()

def calculate_energy(df, time="time", power="Total"):
    dt = df[time].diff().iloc[1:]
    avg_power = (df[power][:-1].values + df[power][1:].values) / 2
    return (avg_power * dt).sum()

def gen_plots(plot_dir, results_dir, name, offset=0, trim=0):
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

    trim_from_end = orig_rows - trim

    diskstat["read"] = diskstat["read"] / 1048576
    diskstat["write"] = diskstat["write"] / 1048576
    diskstat["total"] = diskstat["read"] + diskstat["write"]
    
    ps3 = common.fill_clean(ps3, offset=offset, trim=trim_from_end)
    rapl = common.fill_clean(rapl, offset=offset, trim=trim_from_end)
    sysinfo = common.fill_clean(sysinfo, offset=offset, trim=trim_from_end)
    diskstat = common.fill_clean(diskstat, offset=offset, trim=trim_from_end)

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

    plotter([ps3["time"], ps3["total_smoothed"]], marker_data, os.path.join(base, f"{name}-ssd-power.pdf"), "SSD power (Watts)", offset, trim_from_end)
    plotter([rapl["time"], rapl["total_smoothed"]], marker_data, os.path.join(base, f"{name}-cpu-power.pdf"), "CPU power (Watts)", offset, trim_from_end)
    plotter([diskstat["time"], diskstat["total"]], marker_data, os.path.join(base, f"{name}-diskstat.pdf"), "Iostat throughput (MiB/s)", offset, trim_from_end)
    plotter([diskstat["time"], diskstat["read"]], marker_data, os.path.join(base, f"{name}-read-diskstat.pdf"), "Iostat throughput (MiB/s)", offset, trim_from_end)
    plotter([diskstat["time"], diskstat["write"]], marker_data, os.path.join(base, f"{name}-write-diskstat.pdf"), "Iostat throughput (MiB/s)", offset, trim_from_end)
    plotter([sysinfo["time"], sysinfo["average_freq_node0"]], marker_data, os.path.join(base, f"{name}-cpu-freq-0.pdf"), "CPU frequency (MHz)", offset, trim_from_end)
    plotter([sysinfo["time"], sysinfo["average_load_node0"]], marker_data, os.path.join(base, f"{name}-cpu-load-0.pdf"), "CPU load", offset, trim_from_end)
    plotter([sysinfo["time"], sysinfo["average_freq_node1"]], marker_data, os.path.join(base, f"{name}-cpu-freq-1.pdf"), "CPU frequency (MHz)", offset, trim_from_end)
    plotter([sysinfo["time"], sysinfo["average_load_node1"]], marker_data, os.path.join(base, f"{name}-cpu-load-1.pdf"), "CPU load", offset, trim_from_end)

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
        benchmark_values[item[2]] = item[0][item[1]].mean().item()

    benchmark_value_items = [
        [ps3, "ssd_energy"],
        [rapl, "cpu_energy"]
    ]
    for item in benchmark_value_items:
        benchmark_values[item[1]] = calculate_energy(item[0])


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
    args = parser.parse_args()

    gen_plots(args.plot_dir, args.results_dir, args.name, args.offset, args.trim_end)
