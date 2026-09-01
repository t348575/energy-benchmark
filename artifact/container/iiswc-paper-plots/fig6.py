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

SLIDE_PLOT=False

def diffify(point1, arr, absolute):
    o = []
    for bwar, metricar, erbar, llab in arr:
        if absolute:
            o.append((bwar, [y - point1 for y in metricar], erbar, llab))
        else:
            o.append((bwar, [(y - point1) / point1 for y in metricar], erbar, llab))
    return o

def qd_scaling_plot(ar, target, ssd, ylab, yrange, label, arerr = None, diroverride=None):
    fig, ax = plt.subplots()
    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

    plt.plot(range(1, len(ar)+1), ar, color=colors[1], linewidth=5, marker='o', markersize=8)
    if arerr:
        plt.errorbar(range(1, len(ar)+1), ar, yerr = [abs(y) for y in arerr], fmt ='o', color=colors[1], ecolor=colors[1], elinewidth=3, linewidth=8)

    ax.set_ylabel(bold(ylab))
    plt.ylim(0, yrange)
    plt.xlim(0, 11)
    plt.grid()
    ax.set_xlabel(bold("Queue depth"))
    ax.set_xticks(range(1, len(ar)+1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'][:len(ar)])

    di = diroverride if diroverride else 'qd'
    filename = f'./plots/ioshaping/{di}/{target}/{ssd}-{label}.pdf'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    fig.savefig(filename, bbox_inches="tight")
    print("see ", filename)
    plt.close(fig)

def t_scaling_plot(ar, target, ssd, ylab, yrange, label, arerr = None, diroverride=None):
    fig, ax = plt.subplots()
    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

    plt.plot(range(1, len(ar)+1), ar, color=colors[1], linewidth=5, marker='o', markersize=8)
    if arerr:
        plt.errorbar(range(1, len(ar)+1), ar, yerr = [abs(y) for y in arerr], fmt ='o', color=colors[1], ecolor=colors[1], elinewidth=3, linewidth=8)

    ax.set_ylabel(bold(ylab))
    plt.ylim(0, yrange)
    plt.xlim(0, 11)
    plt.grid()
    ax.set_xlabel(bold("Threads"))
    ax.set_xticks(range(1, len(ar)+1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'][:len(ar)])

    di = diroverride if diroverride else 't'
    filename = f'./plots/ioshaping/{di}/{target}/{ssd}-{label}.pdf'
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    fig.savefig(filename, bbox_inches="tight")
    print("see ", filename)
    plt.close(fig)

def rq_scaling_plot(ar, target, ssd, ylab, yrange, label, arerr = None):
    for divide, metric in [(False, 'rq'), (True, 'iops')]:
        fig, ax = plt.subplots()
        colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]
    
        arf = ar
        if divide:
            divarray = [2**(12+i) for i in range(len(ar))]
            arf = [((x*1024*1024*1024)/y)/1000 for x,y in zip(ar, divarray)]
        plt.plot(range(1, len(arf)+1), arf, color=colors[1], linewidth=5, marker='o', markersize=8)
        if arerr:
            plt.errorbar(range(1, len(arf)+1), arf, yerr = [abs(y) for y in arerr], fmt ='o', color=colors[1], ecolor=colors[1], elinewidth=3, linewidth=8)

        ax.set_ylabel(bold(ylab))
        #plt.ylim(0 if yrange != 20 else -5, yrange)
        plt.ylim(0, yrange)
        if divide:
            plt.ylim(0, 50)
            ax.set_ylabel(bold("Throughput (KIOPS)"))
        plt.xlim(0, 11)
        plt.grid()

        ax.set_xlabel(bold("Request size (KiB)"))
        ax.set_xticks(range(1, len(vs[ssd]['rq-b'])+1), ['4', '8', '16', '32', '64', '128', '256', '512', '1024', '2048', '4096'])

        max_transfer_ssd,_ = maxtransfer[ssd]

        plt.axvline(x=max_transfer_ssd, color='red', ls='--')

        filename = f'./plots/ioshaping/{metric}/{target}/{ssd}-{label}.pdf'
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        fig.savefig(filename, bbox_inches="tight")
        print("see ", filename)
        plt.close(fig)



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

                for wl in ['qd', 't']:
                    if not 'rate' in ssd and 'sync' in vs[ssd] and len(appendix) < 2:
                        for sze in ['4k', '16k', '64k', '1m']:
                            # we call it ducttape, I call it a mistake
                            if 'ssd_d' == ssd and 't' in wl:
                                continue 
                            for en in ['sync', 'posixaio', 'libaio', 'io_uring', 'Polling', 'KernelPolling', 'SPDK']: 
                                if 'qd' in wl:
                                    yerr = vs[ssd][en][sze][f'qd-pdev-cpu'] if 'p-cpu' in metric else None
                                    qd_scaling_plot(vs[ssd][en][sze][f'qd-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', arerr=yerr, diroverride=f"qd/{en}/{sze}")
                                    first = vs[ssd][en][sze][f'qd-{metric}{appendix}'][0]
                                    diff = [y - first for y in vs[ssd][en][sze][f'qd-{metric}{appendix}']] 
                                    qd_scaling_plot(diff, target, ssd, labilify_metric(metric), 20, f'{metric}{appendix}-delta', arerr=yerr, diroverride=f"qd/{en}/{sze}")
                                elif 't-e' in vs[ssd][en][sze]:
                                    if 'SPDK' in en:
                                        continue
                                    yerr = vs[ssd][en][sze][f't-pdev-cpu'] if 'p-cpu' in metric else None
                                    t_scaling_plot(vs[ssd][en][sze][f't-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', arerr=yerr, diroverride=f"t/{en}/{sze}")
                                    first = vs[ssd][en][sze][f't-{metric}{appendix}'][0]
                                    diff = [y - first for y in vs[ssd][en][sze][f't-{metric}{appendix}']] 
                                    t_scaling_plot(diff, target, ssd, labilify_metric(metric), 20, f'{metric}{appendix}-delta', arerr=yerr, diroverride=f"t/{en}/{sze}")
                            
                            if 't' in wl and 'ssd_a' in ssd:
                                continue

                            fig, ax = plt.subplots()
                            colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                            for i, en in enumerate(['libaio', 'io_uring', 'Polling', 'KernelPolling', 'SPDK']):
                                labelen = en
                                if en == "io_uring":
                                    labelen='iou'
                                if en == 'Polling':
                                    labelen='iou + s' 
                                elif en == 'KernelPolling':
                                    labelen='iou + c'
                                if 'Polling' in en and 't' in wl and 'ssd_e' in ssd:
                                    continue
                                if 'SPDK' in en and 't' in wl:
                                    continue
                                plt.plot(vs[ssd][en][sze][f'{wl}-b{appendix}'], vs[ssd][en][sze][f'{wl}-{metric}{appendix}'], color=colors[i+1], linewidth=5, label=bold(labelen), marker='o', markersize=8)
                                    
                            if 'e' in metric and len(metric) == 1:
                                ax.set_ylabel(bold("Efficiency (MiB/J)"))
                                if target == "ssd":
                                    plt.ylim(0, 400)
                                elif target == 'system':
                                    plt.ylim(0, 20)
                                else:
                                    plt.ylim(0, 60)
                            elif 'p-' in metric:
                                ax.set_ylabel(bold("Power (W)"))
                                if target == "ssd":
                                    plt.ylim(0, 20)
                                elif target == "cpu":
                                    plt.ylim(0, 90)     
                                else:
                                    plt.ylim(0, 300)     
                            elif 'l-' in metric:
                                ax.set_ylabel(bold("Load (cores)"))
                                plt.ylim(0, 40)

                            plt.xlim(0, 4)
                            plt.grid()

                            ax.set_xlabel(bold("Bandwidth (GiB/s)"))

                            fig.savefig(f'./plots/fig3/{target}/engine/fig3-{wl}-{sze}-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf', bbox_inches="tight")

                            fig, ax = plt.subplots()
                            colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                            engines_to_plot = ['libaio', 'io_uring', 'Polling', 'KernelPolling', 'SPDK'] if not SLIDE_PLOT else ['io_uring', 'KernelPolling']
                            for i, en in enumerate(engines_to_plot):
                                labelen = en
                                if en == "io_uring":
                                    labelen='io_uring + INT'
                                    if SLIDE_PLOT:
                                        labelen='Interrupts'
                                if en == 'Polling':
                                    labelen='io_uring + spoll' 
                                elif en == 'KernelPolling':
                                    labelen='io_uring + cpoll'
                                    if SLIDE_PLOT:
                                        labelen='Polling'
                                if 'Polling' in en and 't' in wl and 'ssd_e' in ssd:
                                    continue
                                if 'SPDK' in en and 't' in wl:
                                    continue
                                plt.plot(range(1, len(vs[ssd][en][sze][f'{wl}-b{appendix}'])+1), vs[ssd][en][sze][f'{wl}-{metric}{appendix}'], color=colors[i+1], linewidth=5, label=bold(labelen), marker='o', markersize=8)

                            if 'e' in metric and len(metric) == 1:
                                ax.set_ylabel(bold("Efficiency (MiB/J)"))
                                if target == "ssd":
                                    plt.ylim(0, 400)
                                elif target == 'system':
                                    plt.ylim(0, 20)
                                else:
                                    plt.ylim(0, 60)
                            elif 'p-' in metric:
                                ax.set_ylabel(bold("Power (W)"))
                                if target == "ssd":
                                    plt.ylim(0, 20)
                                elif target == "cpu":
                                    plt.ylim(0, 90)
                                else:
                                    plt.ylim(0, 300)     
                            elif 'l-' in metric:
                                ax.set_ylabel(bold("Load (cores)"))
                                plt.ylim(0, 40)

                            plt.grid()
                            if sze == '4k' and (not 'p-' in metric):
                                plt.legend(loc=(0.01, 0.42))

                            ax.set_xlabel(bold("Queue depth"))
                            ax.set_xticks(range(1, len([1,2,4,8,16,32,64,128,256])+1), [bold(qdi) for qdi in ['1', '2', '4', '8', '16', '32', '64', '128', '256']])

                            path = f'./plots/fig3/{target}/engine/fig3-{wl}-{sze}-bw{appendix}-SCALING-{target}-{ssd}-{metric}.pdf'
                            fig.savefig(path, bbox_inches="tight")
                            print(f"see {path}")

                            path = f'./plots/fig3/{target}/engine/fig3-{wl}-{sze}-bw{appendix}-SCALING-{target}-{ssd}-{metric}.png'
                            fig.savefig(path, bbox_inches="tight")
                            print(f"see {path}")