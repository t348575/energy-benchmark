import common
import numpy as np
import matplotlib.pyplot as plt

def gen_plots(data, filepath, x_label_name, labels):
    x = np.arange(len(labels))
    width = 0.25

    _, ax = plt.subplots(figsize=(12, 6))
    bar0 = ax.bar(x - width, data[0], width, label='ps0 (8.5W)')
    bar1 = ax.bar(x, data[1], width, label='ps1 (4.5W)')
    bar2 = ax.bar(x + width, data[2], width, label='ps2 (3.2W)')

    for b in [bar0, bar1, bar2]:
        ax.bar_label(b, fmt='%.2f', padding=3, rotation=45)

    ax.set_ylabel("Latency (ms)")
    ax.set_xlabel(x_label_name)
    ax.set_title(f"Latency for {x_label_name.lower()} vs. power state")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    plt.tight_layout()
    plt.savefig(filepath, format='pdf')