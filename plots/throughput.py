import common
import json
import numpy as np
import matplotlib.pyplot as plt

def gen_plots(data, filepath, x_label_name, experiment_name, labels):
    width = 0.25
    x = np.arange(len(labels))

    _, ax = plt.subplots()
    for i in range(len(data)):
        if i == 0:
            s = x - width
        elif i == 1:
            s = x
        else:
            s = x + width
        ax.bar(s, data[i], width, label=common.power_states[i])

    ax.set_ylabel("Throughput (MiB/s)")
    ax.set_xlabel(f"{x_label_name} {experiment_name}")
    ax.set_title(f"Throughput for {x_label_name.lower()} vs. power state")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    plt.savefig(filepath, format='pdf')
    plt.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, required=True)
    parser.add_argument("--filepath", type=str, required=True)
    parser.add_argument("--x_label_name", type=str, required=True)
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--labels", type=str, required=True)
    args = parser.parse_args()

    f = open(args.data, "r")
    data = json.loads(f.read())

    gen_plots(data, args.filepath, args.x_label_name, args.experiment_name, args.labels.split(","))