import common
import json
import numpy as np
import matplotlib.pyplot as plt

def gen_plots(data, filepath, x_label_name, experiment_name, labels, name=""):
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

    ax.set_ylabel("Power (Watts)")
    ax.set_xlabel(f"{x_label_name}")
    ax.set_title(f"{name} power vs. {x_label_name.lower()}")
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
    parser.add_argument("--name", type=str)
    args = parser.parse_args()

    f = open(args.data, "r")
    data = json.loads(f.read())

    if args.name is None:
        args.name = ""

    gen_plots(data, args.filepath, args.x_label_name, args.experiment_name, args.labels.split(","), args.name)