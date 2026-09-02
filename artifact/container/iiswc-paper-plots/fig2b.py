import matplotlib.pyplot as plt

from common.plot_utils import *
set_standard_font()

colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

def plot_power_range(LABELS, y1, y2, filename, ylim):
    fig, ax = plt.subplots()
    y1 = [x1 - x2 for x1,x2 in zip(y1,y2)]
    plt.bar([bold(x) for x in LABELS], y1, bottom=y2, color=colors, linewidth=1, edgecolor='black')
    plt.ylim(0, ylim)
    plt.xlim(-1, len(LABELS))
    plt.grid()
    ax.set_xlabel(bold("Hardware component"))
    ax.set_ylabel(bold("Power (W)"))
    path=f'./plots/{filename}'
    save_iiswc_fig(fig, path)

plot_power_range(['SSDs', 'CPU', 'PDU1', 'PDU2'], [13.6, 167.5, 128.76, 131.9], [0.77, 24.8, 87.7, 79.16], 'server-range-per-PDU', 200)
plot_power_range(['SSDs', 'CPU', 'Server'], [13.6, 167.5, 260.71], [0.45, 24.8, 167], 'server-range-merged-PDU', 300)
