import common
import json
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def gen_plots(matrix, filepath, col_labels, x_label, experiment_name, title, reverse=False):
    if reverse:
        r = "_r"
    else:
        r = ""
    df = pd.DataFrame(matrix, index=col_labels, columns=['ps0 (8.5W)', 'ps1 (4.5W)', 'ps2 (3.2W)'][0:len(matrix[0])])
    df = df.T
    plt.figure(figsize=(12, 4.8))
    g = sns.heatmap(
        df,
        cmap=f"viridis{r}",
        annot=True,
        fmt=".4f",
        linewidths=0.5,
        linecolor="white"
    )
    g.set_yticklabels(g.get_yticklabels(), rotation=45)
    plt.title(f"{x_label} {experiment_name} {title} efficiency")
    plt.ylabel("Power state")
    plt.xlabel(x_label)

    plt.tight_layout()
    plt.savefig(filepath, format="pdf")
    plt.close()

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--data", type=str, required=True)
    parser.add_argument("--filepath", type=str, required=True)
    parser.add_argument("--col_labels", type=str, required=True)
    parser.add_argument("--x_label", type=str, required=True)
    parser.add_argument("--experiment_name", type=str, required=True)
    parser.add_argument("--title", type=str, required=True)
    parser.add_argument("--reverse", type=str, required=False)
    args = parser.parse_args()

    f = open(args.data, "r")
    data = json.loads(f.read())

    if args.reverse == "1":
        reverse = True
    else:
        reverse = False

    gen_plots(data, args.filepath, args.col_labels.split(","), args.x_label, args.experiment_name, args.title, reverse)