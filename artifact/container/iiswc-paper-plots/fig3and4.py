import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os

from common.plot_utils import *

set_standard_font()

maxtransfer = {
    'ssd_e': (6, '128'),
    'ssd_d': (10, '2048'),
    'ssd_c':  (9, '1024'),
    'ssd_a':  (7, '256')

}
pst_ssd_c="nvme-pst-eff-2025-11-16_18-44-20"; ssd_c_psts=2
pst_ssd_e="nvme-pst-eff-2025-11-18_12-32-49"; ssd_e_psts=2
pst_ssd_a="nvme-pst-eff-2025-12-02_00-57-09"; ssd_a_psts=3
psts_ran = {
    'ssd_c' : ssd_c_psts,
    'ssd_e' : ssd_e_psts,
    'ssd_a': ssd_a_psts
}

def diffify(point1, arr, absolute):
    o = []
    for bwar, metricar, erbar, llab in arr:
        if absolute:
            o.append((bwar, [y - point1 for y in metricar], erbar, llab))
        else:
            o.append((bwar, [(y - point1) / point1 for y in metricar], erbar, llab))
    return o

def bw_scaling_plot(lines, target, ssd, ylab, yrange, label):
    fig, ax = plt.subplots()
    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA, 'black', 'gray']

    i = 0
    for bwar, metricar, berr, llab in lines:
        plt.plot(bwar, metricar, color=colors[i], linewidth=8, marker='o', markersize=12, label=bold(llab))
        if berr:
            plt.errorbar(bwar, metricar, yerr = [abs(y) for y in berr], fmt ='o', color=colors[i], ecolor=colors[i], elinewidth=4, linewidth=12)
        i = i + 1
    plt.ylim(0, yrange)
    if yrange == 700:
        ax.set_yticks([200, 400, 600, 700], [bold(yy) for yy in ['200', '400', '600', '700']])
    if yrange == 90:
        ax.set_yticks([20, 40, 60, 80, 90], [bold(yy) for yy in ['20', '40', '60', '80', '90']])
    
    plt.grid()

    filename = f'./plots/ioshaping/bw/{target}/{ssd}-{label}.png'

    if 'PS' in filename:
        ax.set_xlabel(bold("Bandwidth (GiB/s)"))
        ax.set_ylabel(bold(ylab))
    else:
        ax.set_xlabel(bold("Bandwidth (GiB/s)"), fontsize=32)
        ax.set_ylabel(bold(ylab), fontsize=32)
        ax.tick_params(axis='both', which='major', labelsize=32)

    if 'ssd_a' in filename:
        if 'PS' in filename:
            plt.legend(loc='lower right')
        elif 'ssd' in filename and '-e' in filename:
            plt.legend(fontsize=28, columnspacing=-0.1, loc=(0.25, 0.01), handletextpad=0.1)
        elif 'cpu' in filename and '-e' in filename:
            plt.legend(fontsize=28, columnspacing=-0.1, loc=(0.01, 0.52))
        else:
            plt.legend(fontsize=28, columnspacing=-0.1)
    
    plt.xlim(0, 4 if not 'PS' in filename else 1)
    save_iiswc_fig(fig, filename.split('.pdf')[0])
    
    if 'delta-p-cpu' in filename:
        o.append((filename, lines[2][1][::], lines[2][-1]))

def labilify_metric(metric):
    if 'e' in metric and len(metric) == 1:
        return "Efficiency (MiB/J)"
    elif 'p-' in metric:
        return "Power (W)"
    elif 'l-' in metric:
        return "Load (cores)"
    return "Unknown"

def rangify_metric(metric, target):
    if 'e' in metric and len(metric) == 1:
        if target == "ssd":
            return 700
        elif target == 'system':
            return 20
        else:
            return 80
    elif 'p-' in metric:
        if target == "ssd":
            return 20
        elif target == "cpu":
            return 90 
        else:
            return 300 
    elif 'l-' in metric:
        return 40
    return 0

ws={}
with open(f"iiswcdata/preprocessed-ebench-data.json", "r") as f:
    ws = json.load(f)

for target in ['cpu', 'ssd', 'both', 'system']:
    vs = ws[target]

    # SSDs scatter for slides
    fig, ax = plt.subplots()
    
    COLORS = [ROSE, CYAN, SAND, TEAL, MAGENTA]
    for i, ssd in enumerate(['ssd_a', 'ssd_d', 'ssd_c', 'ssd_e']):
        ax.scatter(vs[ssd][f'qd-b'] + vs[ssd][f'rq-b'] + vs[ssd][f't-b'], vs[ssd][f'qd-e'] + vs[ssd][f'rq-e'] + vs[ssd][f't-e'], color=COLORS[i], s=100, label=ssd.split('_')[1].upper())
        
    ax.set_xlabel(bold("Bandwidth (GiB/s)"), fontsize=32)
    ax.set_ylabel(bold("Efficiency (MiB/J)"), fontsize=32)
    ax.legend(fontsize=32,title="SSD",title_fontsize=32,borderpad=0.01,labelspacing=0.1)
    ax.grid()
    path=f'./plots/ioshaping/bw/{target}/all-ssds-scatter'
    save_iiswc_fig(fig, path)

    # Plots for paper
    for appendix in ['', '-seq']:
        for ssd in ['ssd_a', 'ssd_d', 'ssd_c', 'ssd_e', 'ssd_c_rate', 'ssd_d_rate', 'ssd_e_rate']:

            # We have to skip some combinations of target and ssd because they take too long to run or are not relevant to our final plots. 
            #for metric in [('e'), ('p-cpu'), ('p-ssd'), ('l-cpu'), ('p-both'), ('p-system')]:
            for metric in [('e')]:
                if 'ssd' in target and 'cpu' in metric:
                    continue
                if 'cpu' in target and 'ssd' in metric:
                    continue
                if 'both' in metric and not 'both' in target:
                    continue
                if ('ssd' in metric or 'cpu' in metric or 'system' in metric) and 'both' in target:
                    continue
                if 'system' in metric and not 'system' in target:
                    continue
                if ('ssd' in metric or 'cpu' in metric or 'both' in metric) and 'system' in target:
                    continue
                if 'rate' in ssd and len(appendix) > 1:
                    continue

                # BW plot
                lines = []
                
                bstdev = None
                if 'p-cpu' in metric:
                    bstdev = vs[ssd][f'qd-pdev-cpu{appendix}']
                elif 'p-ssd' in metric:
                    bstdev = vs[ssd][f'qd-pdev-ssd{appendix}']
                lines.append(
                    (vs[ssd][f'qd-b{appendix}'], vs[ssd][f'qd-{metric}{appendix}'], bstdev, "Queue Depth"))
                if (not 'rate' in ssd) or ('ssd_e' in ssd):
                    bstdev = None
                    if 'p-cpu' in metric:
                        bstdev = vs[ssd][f'rq-pdev-cpu{appendix}']
                    elif 'p-ssd' in metric:
                        bstdev = vs[ssd][f'rq-pdev-ssd{appendix}']
                    lines.append(
                        (vs[ssd][f'rq-b{appendix}'], vs[ssd][f'rq-{metric}{appendix}'], bstdev, "Request size"))

                # Our preprocessed data set does not have all data.        
                if len(appendix) < 2 or ssd == 'ssd_c':
                    bstdev = None
                    if 'p-cpu' in metric and not 'seq' in appendix:
                        bstdev = vs[ssd][f't-pdev-cpu{appendix}']
                    elif 'p-ssd' in metric and not 'seq' in appendix:
                        bstdev = vs[ssd][f't-pdev-ssd{appendix}']
                    b = 'seqread-threads' if appendix == '-seq' else ''
                    lines.append(
                        (vs[ssd][f't{b}-b'], vs[ssd][f't{b}-{metric}'], bstdev, "Threads"))

                    bw_scaling_plot(lines, target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'absolute-{metric}{appendix}')
                    first = vs[ssd][f'qd-{metric}{appendix}'][0]
                    bw_scaling_plot(diffify(first, lines, True), target, ssd, labilify_metric(metric), 20, f'delta-{metric}{appendix}')
                    bw_scaling_plot(diffify(first, lines, False), target, ssd, labilify_metric(metric), 10, f'deltap-{metric}{appendix}')

                    if ssd == 'ssd_c' and len(appendix) < 2:
                        lines.append(
                            (vs[ssd][f'tsinglecore-b'], vs[ssd][f't{b}-{metric}'], None, "Threads C"))
                        lines.append(
                            (vs[ssd][f'tround-robin-b'], vs[ssd][f't{b}-{metric}'], None, "Threads RR"))
                        bw_scaling_plot(lines, target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'rr-absolute-{metric}{appendix}')
                    
                    if not 'rate' in ssd and 'ps0' in vs[ssd]:
                        for pattern in ['qd', 'rq']:
                            lines = []
                            for ps in [f'ps{i}' for i in range(psts_ran[ssd])]:
                                lines.append(
                                    (vs[ssd][ps][f'{pattern}-b'], vs[ssd][ps][f'{pattern}-{metric}'][:8], vs[ssd][ps][f'{pattern}-pdev-cpu'] if 'p-cpu' in metric else None, f"PS{ps[-1]}"))
                            try:
                                bw_scaling_plot(lines, target, ssd, labilify_metric(metric), 300, f'absolute-{metric}-PS-{pattern}')
                            except:
                                print(lines, target, ssd)
                                print('failed')