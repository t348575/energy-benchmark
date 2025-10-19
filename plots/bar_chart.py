import json
import argparse
from typing import List

import numpy as np
import matplotlib.pyplot as plt

import common

def get_plot_legends(data_series: List[List[float]], legend_labels, default_legends):
    series_count = len(data_series)
    if not legend_labels:
        return default_legends[:series_count]

    legends = list(legend_labels)
    if len(legends) < series_count:
        remaining = series_count - len(legends)
        legends.extend(default_legends[len(legends):len(legends) + remaining])
    return legends[:series_count]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--spec", required=True, help="Path to JSON bar chart specification")
    args = parser.parse_args()

    with open(args.spec, "r", encoding="utf-8") as f:
        spec = json.load(f)

    data = spec["data"]
    if not data:
        raise ValueError("No data provided")

    with open(args.spec, "r", encoding="utf-8") as f:
        spec = json.load(f)

    labels = spec["labels"]
    width = spec.get("bar_width")
    series_count = len(data)
    if width is None:
        width = 0.8 / max(1, series_count)

    legends = get_plot_legends(data, spec.get("legend_labels"), spec.get("nvme_power_states"))

    x = np.arange(len(labels))
    offsets = np.linspace(-(series_count - 1) / 2, (series_count - 1) / 2, series_count) * width

    _, ax = plt.subplots()
    for idx, series in enumerate(data):
        offset = offsets[idx] if series_count > 1 else 0
        positions = x + offset
        color = common.colors[idx % len(common.colors)]
        ax.bar(positions, series, width, label=legends[idx], color=color)

    ax.set_ylabel(spec["y_label"])
    ax.set_xlabel(spec["x_label"])
    ax.set_title(spec["title"])
    ax.set_xticks(x)
    ax.set_xticklabels(labels)

    rotation = spec.get("tick_rotation_deg")
    if rotation is not None:
        align = spec.get("tick_horizontal_align", "center")
        plt.setp(ax.get_xticklabels(), rotation=rotation, ha=align)

    ax.legend()
    plt.tight_layout()
    plt.savefig(spec["output_path"], format=spec.get("format", "pdf"))
    plt.close()
