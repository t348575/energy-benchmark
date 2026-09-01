import matplotlib.pyplot as plt
import numpy as np
import json

from common.plot_utils import *

set_standard_font()
colors = [ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

def smoothen_line(x, y, smoothen_step):
    xlen = int((x[-1] - x[0] + 1) / smoothen_step) + 1 
    newx = [float(i) * smoothen_step + x[0] for i in range(xlen)]
    newy = [[0,0] for i in range(xlen)]

    # Count into bins
    first = (x[0] // smoothen_step) * smoothen_step
    for i,j in zip(x,y):
        roundedi = (i // smoothen_step) * smoothen_step + smoothen_step * 0.5
        roundedi = int((roundedi - first) / smoothen_step)
        newy[roundedi] = [newy[roundedi][0] + 1, newy[roundedi][1] + j]
    
    # Collapse
    newy = [i[1] / i[0] if i[0] else 0 for i in newy]
    return (newx, newy)

for out in ['randw-ssd_a', 'randw-ssd_d', 'rw-ssd_d', 'rw-ssd_a',     'seqw-ssd_d', 'seqw-ssd_a', 
        ]:
    with open(f"./iiswcdata/preprocessed-fig5cd-{out}.json", 'r') as f:
        data = json.load(f)
        bw_x = np.array(data['bw_x'])
        bw_y = np.array(data['bw_y'])
        p_x = np.array(data['p_x'])
        p_y = np.array(data['p_y'])

    sbw_x, sbw_y = smoothen_line(bw_x, bw_y, 1)
    sp_x, sp_y = smoothen_line(p_x, p_y, 1)

    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    plt.grid()

    ax1.set_xlabel(bold('Time (minutes)'), fontsize=24)
    ax1.grid(True)
    ax2.grid(False)
    ax1.set_ylabel(bold('Bandwidth (GiB/s)'), fontsize=24)
    ax2.set_ylabel(bold('Power (W)'), fontsize=24)

    ax1.plot(sbw_x[1:-1], sbw_y[1:-1], color=colors[0], linewidth=5)
    ax2.plot(sp_x[1:-1], sp_y[1:-1], color=colors[1], linewidth=5)

    ax1.set_xticks([0, 600, 1200, 1800], [bold(x) for x in ['0', '10', '20', '30']])

    ax1.legend(labels=[bold('Bandwidth')], loc='upper left')
    ax2.legend(labels=[bold('Power')], loc='upper right')
    ax1.set_ylim(0, 2)
    ax2.set_ylim(0, 10)
    
    path = f'./plots/{out}.pdf'
    fig.savefig(path, bbox_inches="tight")
    print("see ", path)


