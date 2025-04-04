import common
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def gen_plots(fname, matrix, col_labels, x_label, title):
    df = pd.DataFrame(matrix, index=col_labels, columns=['ps0 (8.5W)', 'ps1 (4.5W)', 'ps2 (3.2W)'])
    df = df.T
    plt.figure(figsize=(12, 5))
    g = sns.heatmap(
        df,
        cmap="YlGnBu",
        annot=True,
        fmt=".2f",
        linewidths=0.5,
        linecolor="white"
    )
    g.set_yticklabels(g.get_yticklabels(), rotation=45)
    plt.title(f"{x_label} {title} efficiency")
    plt.ylabel("Power state")
    plt.xlabel(x_label)

    plt.tight_layout()
    plt.savefig(fname, format="pdf")