import matplotlib.pyplot as plt

from common.plot_utils import *

set_standard_font()
colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

SLIDE_PLOT=False

LABELS=['XFS', 'EXT4', 'F2FS']
data = [\
        ([[22493.638387461975, 21417.959444566604, 23393.588840244178], [20166.442467666217, 18304.5463882734, 19260.59973785481]], 'ssd', 40), \
        ([[1571.8600666721775, 1464.15211007408, 1370.7923728562052], [1696.325725868494, 1561.7377569078783, 1471.336757361558]], 'cpu', 2), \
        ([[580.2167173692706, 545.5258037261943, 494.41210904899134], [620.9242665276951, 573.967880130074, 512.860513895487]], 'server', 1)]

# Paper
for ys, wl, ran in data:
    fig, ax = plt.subplots()

    j = 0
    for i in range(3 if not SLIDE_PLOT else 1):
        plt.bar(j - 0.25, ys[0][i] / 1000, width=0.45, color=colors[0], linewidth=1, edgecolor='black')
        plt.bar(j + 0.25, ys[1][i] / 1000, width=0.45, color=colors[1], linewidth=1, edgecolor='black')
        j = j + 2

    plt.ylim(0, ran)
    plt.xlim(-1, 5 if not SLIDE_PLOT else 1)
    plt.grid()

    if not SLIDE_PLOT:
        ax.set_xticks([x*2 for x in range(len(LABELS))], [bold(b) for b in LABELS], fontsize=28)
        ax.tick_params(axis='both', which='major', labelsize=32)
    else:
        ax.set_xticks([], [], fontsize=28)
        ax.tick_params(axis='both', which='major', labelsize=32)
    ax.set_ylabel(bold(" Efficiency (KIOPS/J)"), fontsize=32)
    if (ran > 10):
        plt.legend(labels=[bold(b) for b in ['D', 'E']], ncol=2, fontsize=32)

    path = f'./plots/filebench-{wl}'
    save_iiswc_fig(fig, path)