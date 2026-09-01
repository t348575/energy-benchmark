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

                if 'enhz' in vs[ssd] and len(appendix) < 2:
                    for bw in [False, True]:
                        for qd in [1, 256]:
                            for sz in ['4k', '1m']:
                                fig, ax = plt.subplots()
                                colors = [ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                                ci = 0
                                for ei, engine in [(0, 'libaio'), (-1, 'io_uring + INT'), (1, 'poll'), (2, 'kernelpoll'), (-1, 'SPDK')]:
                                    x = []
                                    y = []
                                    z = []
                                    for di, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz', 'default']):
                                        x.append(di+0.5)
                                        if 'def' in dvfs:
                                            jmp = 0 if qd == 1 else -1
                                            enginestr = ""
                                            if 'aio' in engine or 'SPDK' in engine:
                                                enginestr = engine
                                            elif 'poll' in engine:
                                                enginestr = "Polling" if not 'kernel' in engine else "KernelPolling" 
                                            else:
                                                enginestr = "io_uring"
                                            y.append(vs[ssd][enginestr][sz][f"qd-{metric}"][jmp])
                                            z.append(vs[ssd][enginestr][sz][f"qd-b"][jmp])
                                        else:
                                            if 'INT' in engine:
                                                jmp = 0 if 'k' in sz else 9
                                                if qd > 1:
                                                    jmp = jmp + 7
                                                y.append(vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][jmp])
                                                z.append(vs[ssd][dvfs][f'iodepth-b{appendix}'][jmp])
                                            elif 'SPDK' in engine:
                                                jmp = 0 if 'k' in sz else 1
                                                if qd > 1:
                                                    jmp = jmp + 2
                                                y.append(vs[ssd]['spdkhz'][dvfs][f'iodepth-{metric}{appendix}'][jmp])
                                                jmp = 0 if 'k' in sz else 2
                                                if qd > 1:
                                                    jmp = jmp + 1
                                                z.append(vs[ssd]['spdkhz'][dvfs][f'iodepth-b{appendix}'][jmp])
                                            else:
                                                jmp = 1 if 'm' in sz else 0
                                                if qd > 1:
                                                    jmp = jmp + 6
                                                realei = {
                                                    0: 2,
                                                    1: 0,
                                                    2: 1 
                                                }[ei]
                                                y.append(vs[ssd]['enhz'][dvfs][f'iodepth-{metric}{appendix}'][2*realei + jmp])
                                                jmp = 6 if 'm' in sz else 0
                                                if qd > 1:
                                                    jmp = jmp + 1
                                                z.append(vs[ssd]['enhz'][dvfs][f'iodepth-b{appendix}'][2*ei + jmp])
                                    lab = engine 
                                    if engine == 'poll':
                                        lab = 'io_uring + spoll'
                                    elif engine == 'kernelpoll':
                                        lab = 'io_uring + cpoll'
                                    plt.plot(x, y if not bw else z, color=colors[ci], linewidth=5, marker='o', markersize=8, label=bold(lab))
                                    ci = ci + 1

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
                                    else:
                                        plt.ylim(0, 300)     
                                elif 'l-' in metric:
                                    ax.set_ylabel(bold("Load (cores)"))
                                    plt.ylim(0, 40)
                                if bw:
                                    ax.set_ylabel(bold("Bandwidth (GiB/s)"))
                                    plt.ylim(0, 4)

                                plt.grid()

                                filename=f'DVFS-{"bw-" if bw else ""}ENG-{sz}-{qd}-fig3-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf'
                                if sz == '4k' and qd == 256 and (not bw):
                                    filename=f'finaldvfs4k-{ssd}'
                                if sz == '1m' and qd == 256 and (not bw):   
                                    plt.legend(loc=(0.01,0.01), labelspacing=0.3)
                                    filename=f'finaldvfs1m-{ssd}'

                                ax.set_xticks([x + 0.5 for x in range(0, 7)], [bold(fq) for fq in ['1.00', '1.44', '1.88', '2.32', '2.76', '3.20', 'Schedutil']]) 
                                ax.set_xlabel(bold("CPU Frequency (GHz)"), labelpad=-30, x=0.4)
                                plt.xticks(rotation=45)

                                path=f'./plots/fig3/{target}/{filename}.pdf'
                                fig.savefig(path, bbox_inches="tight")
                                print("see ", path)

            # DVFS
                if not 'rate' in ssd and '1ghz' in vs[ssd] and len(appendix) < 2:
                    for l1,l2,slab in [(0, 8, '4k'), (9, 17, '1m')]:
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            glab = {
                                '1ghz':    '1.00GHz',
                                '1.44ghz': '1.44GHz',
                                '1.88ghz': '1.88GHz',
                                '2.32ghz': '2.32GHz',
                                '2.76ghz': '2.76GHz',
                                '3.2ghz':  '3.20GHz'
                            }[dvfs]
                            plt.plot(vs[ssd][dvfs][f'iodepth-b{appendix}'][l1:l2], vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(glab), marker='o', markersize=8)
                        # The missing link
                        if '4k' in slab:
                            plt.plot(vs[ssd][f'qd-b{appendix}'], vs[ssd][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('None'), marker='o', markersize=8)
                                
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
                            else:
                                plt.ylim(0, 300)     
                        elif 'l-' in metric:
                            ax.set_ylabel(bold("Load (cores)"))
                            plt.ylim(0, 40)

                        plt.xlim(0, 4)
                        plt.grid()
                        plt.legend()

                        ax.set_xlabel(bold("Bandwidth (GiB/s)"))

                        fig.savefig(f'./plots/fig3/{target}/DVFS-{slab}-fig3-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf', bbox_inches="tight")
                    
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            glab = {
                                '1ghz':    '1.00GHz',
                                '1.44ghz': '1.44GHz',
                                '1.88ghz': '1.88GHz',
                                '2.32ghz': '2.32GHz',
                                '2.76ghz': '2.76GHz',
                                '3.2ghz':  '3.20GHz'
                            }[dvfs]
                            plt.plot([x - 0.5 for x in range(1, len(vs[ssd][dvfs][f'iodepth-b{appendix}'][l1:l2+1])+1)], vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][l1:l2+1], color=colors[i], linewidth=5, label=bold(glab), marker='o', markersize=8)
                        # The missing link
                        if '4k' in slab:
                            plt.plot([x - 0.5 for x in range(1, len(vs[ssd][f'qd-b{appendix}'])+1)], vs[ssd][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('Schedutil'), marker='o', markersize=8)
                        elif '1m' in slab and 'ssd_d' in ssd:
                            plt.plot([x - 0.5 for x in range(1, len(vs[ssd]['io_uring']['1m'][f'qd-b{appendix}'])+1)], vs[ssd]['io_uring']['1m'][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('Schedutil'), marker='o', markersize=8)
                                
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
                            else:
                                plt.ylim(0, 70)     
                        elif 'l-' in metric:
                            ax.set_ylabel(bold("Load (cores)"))
                            plt.ylim(0, 40)

                        plt.xlim(0, 9)
                        plt.grid()
                        if '4k' in slab:
                            plt.legend(ncol=2, loc=(0.01,0.53), columnspacing=0.9)

                        ax.set_xlabel(bold("Queue depth"))
                        ax.set_xticks([x + 0.5 for x in range(0, 9)], [bold(qdi) for qdi in ['1', '2', '4', '8', '16', '32', '64', '128', '256']])

                        path=f'./plots/fig3/{target}/DVFS-QD-{slab}-fig3-bw{appendix}-{target}-{ssd}-{metric}.pdf'
                        fig.savefig(path, bbox_inches="tight")
                        print(f"see {path}")

                    for l1,l2,slab in [(0, 8, '4k'), (9, 17, '1m')]:
                        if 'ssd_d' in ssd:
                            break
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(vs[ssd][dvfs][f'threads-b{appendix}'][l1:l2], vs[ssd][dvfs][f'threads-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
                        # The missing link
                        if '4k' in slab:
                            plt.plot(vs[ssd][f't-b{appendix}'], vs[ssd][f't-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('None'), marker='o', markersize=8)
                                
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
                            else:
                                plt.ylim(0, 300)     
                        elif 'l-' in metric:
                            ax.set_ylabel(bold("Load (cores)"))
                            plt.ylim(0, 40)

                        plt.xlim(0, 4)
                        plt.grid()
                        plt.legend()

                        ax.set_xlabel(bold("Bandwidth (GiB/s)"))

                        fig.savefig(f'./plots/fig3/{target}/DVFST-{slab}-fig3-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf', bbox_inches="tight")
                    

                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(range(1, len(vs[ssd][dvfs][f'threads-b{appendix}'][l1:l2])+1), vs[ssd][dvfs][f'threads-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
                        # The missing link
                        if '4k' in slab:
                            plt.plot(range(1, len(vs[ssd][f't-b{appendix}'])+1), vs[ssd][f't-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('None'), marker='o', markersize=8)
                                
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
                            else:
                                plt.ylim(0, 300)     
                        elif 'l-' in metric:
                            ax.set_ylabel(bold("Load (cores)"))
                            plt.ylim(0, 40)

                        plt.xlim(0, 4)
                        plt.grid()
                        plt.legend()

                        ax.set_xlabel(bold("Queue depth"))
                        ax.set_xticks(range(1, 10), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])
