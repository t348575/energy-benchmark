import json
import matplotlib.pyplot as plt
from common.plot_utils import *

set_standard_font()

maxtransfer = {
    'ssd_e': (6, '128'),
    'ssd_d': (10, '2048'),
    'ssd_c': (9, '1024'),
    'ssd_a': (7, '256'),
}

psts_ran = {
    'ssd_c': 2,
    'ssd_e': 2,
    'ssd_a': 3,
}


def diffify(point1, arr, absolute):
    o = []
    for bwar, metricar, erbar, llab in arr:
        if absolute:
            o.append((bwar, [y - point1 for y in metricar], erbar, llab))
        else:
            o.append((bwar, [(y - point1) / point1 for y in metricar], erbar, llab))
    return o


def trendline(arr):
    o = [1]
    for i in range(1, len(arr)):
        o.append(arr[i] / arr[i - 1])
    return o


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


def bw_scaling_plot(lines, target, ssd, ylab, yrange, label):
    fig, ax = plt.subplots()
    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA, 'black', 'gray']

    i = 0
    for bwar, metricar, berr, llab in lines:
        plt.plot(bwar, metricar, color=colors[i], linewidth=8, marker='o', markersize=12, label=bold(llab))
        if berr:
            plt.errorbar(bwar, metricar, yerr=[abs(y) for y in berr], fmt='o', color=colors[i], ecolor=colors[i], elinewidth=4, linewidth=12)
        i = i + 1
    ax.set_ylabel(bold(ylab), fontsize=32)
    plt.ylim(0, yrange)
    if yrange == 700:
        ax.set_yticks([200, 400, 600, 700], [bold(yy) for yy in ['200', '400', '600', '700']])
    if yrange == 90:
        ax.set_yticks([20, 40, 60, 80, 90], [bold(yy) for yy in ['20', '40', '60', '80', '90']])

    plt.xlim(0, 4)
    plt.grid()

    ax.set_xlabel(bold("Bandwidth (GiB/s)"), fontsize=32)
    ax.tick_params(axis='both', which='major', labelsize=32)

    filename = f'./plots/ioshaping/bw/{target}/{ssd}-{label}'
    if 'ssd_a' in filename:
        if 'ssd' in filename and '-e' in filename:
            plt.legend(fontsize=28, columnspacing=-0.1, loc=(0.25, 0.01), handletextpad=0.1)
        elif 'cpu' in filename and '-e' in filename:
            plt.legend(fontsize=28, columnspacing=-0.1, loc=(0.01, 0.52))
        else:
            plt.legend(fontsize=28, columnspacing=-0.1)

    save_iiswc_fig(fig, filename)


def qd_scaling_plot(ar, target, ssd, ylab, yrange, label, arerr=None, diroverride=None):
    fig, ax = plt.subplots()
    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

    plt.plot(range(1, len(ar) + 1), ar, color=colors[1], linewidth=5, marker='o', markersize=8)
    if arerr:
        plt.errorbar(range(1, len(ar) + 1), ar, yerr=[abs(y) for y in arerr], fmt='o', color=colors[1], ecolor=colors[1], elinewidth=3, linewidth=8)

    ax.set_ylabel(bold(ylab))
    plt.ylim(0, yrange)
    plt.xlim(0, 11)
    plt.grid()
    ax.set_xlabel(bold("Queue depth"))
    ax.set_xticks(range(1, len(ar) + 1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'][:len(ar)])

    di = diroverride if diroverride else 'qd'
    filename = f'./plots/ioshaping/{di}/{target}/{ssd}-{label}'
    save_iiswc_fig(fig, filename)


def t_scaling_plot(ar, target, ssd, ylab, yrange, label, arerr=None, diroverride=None):
    fig, ax = plt.subplots()
    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

    plt.plot(range(1, len(ar) + 1), ar, color=colors[1], linewidth=5, marker='o', markersize=8)
    if arerr:
        plt.errorbar(range(1, len(ar) + 1), ar, yerr=[abs(y) for y in arerr], fmt='o', color=colors[1], ecolor=colors[1], elinewidth=3, linewidth=8)

    ax.set_ylabel(bold(ylab))
    plt.ylim(0, yrange)
    plt.xlim(0, 11)
    plt.grid()
    ax.set_xlabel(bold("Threads"))
    ax.set_xticks(range(1, len(ar) + 1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'][:len(ar)])

    di = diroverride if diroverride else 't'
    filename = f'./plots/ioshaping/{di}/{target}/{ssd}-{label}'
    save_iiswc_fig(fig, filename)


def rq_scaling_plot(ar, target, ssd, ylab, yrange, label, arerr=None):
    for divide, metric in [(False, 'rq'), (True, 'iops')]:
        fig, ax = plt.subplots()
        colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

        arf = ar
        if divide:
            divarray = [2 ** (12 + i) for i in range(len(ar))]
            arf = [((x * 1024 * 1024 * 1024) / y) / 1000 for x, y in zip(ar, divarray)]
        plt.plot(range(1, len(arf) + 1), arf, color=colors[1], linewidth=5, marker='o', markersize=8)
        if arerr:
            plt.errorbar(range(1, len(arf) + 1), arf, yerr=[abs(y) for y in arerr], fmt='o', color=colors[1], ecolor=colors[1], elinewidth=3, linewidth=8)

        ax.set_ylabel(bold(ylab))
        plt.ylim(0, yrange)
        if divide:
            plt.ylim(0, 50)
            ax.set_ylabel(bold("Throughput (KIOPS)"))
        plt.xlim(0, 11)
        plt.grid()

        ax.set_xlabel(bold("Request size (KiB)"))
        ax.set_xticks(range(1, len(vs[ssd]['rq-b']) + 1), ['4', '8', '16', '32', '64', '128', '256', '512', '1024', '2048', '4096'])

        max_transfer_ssd, _ = maxtransfer[ssd]
        plt.axvline(x=max_transfer_ssd, color='red', ls='--')

        filename = f'./plots/ioshaping/{metric}/{target}/{ssd}-{label}'
        save_iiswc_fig(fig, filename)


with open("iiswcdata/preprocessed-ebench-data.json", "r") as f:
    ws = json.load(f)

for target in ['cpu', 'ssd', 'both', 'system']:
    vs = ws[target]

    for appendix in ['', '-seq']:
        for ssd in ['ssd_d', 'ssd_a', 'ssd_e']:
            for metric in [('e'), ('p-cpu'), ('p-ssd'), ('l-cpu'), ('p-both'), ('p-system')]:
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
                if True:
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
                                    ...

                # RQ size plot
                if not 'rate' in ssd:
                    if 'e' in metric and len(metric) == 1:
                        rq_scaling_plot(vs[ssd][f'rq-b{appendix}'], target, ssd, "Bandwidth (GiB/s)", 4, f'bw{appendix}')
                        first = vs[ssd][f'rq-b{appendix}'][0]
                        diff = [y - first for y in vs[ssd][f'rq-b{appendix}']]
                        rq_scaling_plot(diff, target, ssd, "Bandwidth (GiB/s) delta", 4, f'diff-bw{appendix}')
                        diffp = [(y - first) / first for y in vs[ssd][f'rq-b{appendix}']]
                        rq_scaling_plot(diffp, target, ssd, "Bandwidth (GiB/s) per inc", 4, f'diffp-bw{appendix}')
                    yerr = vs[ssd][f'rq-pdev-cpu{appendix}'] if 'p-cpu' in metric else None
                    rq_scaling_plot(vs[ssd][f'rq-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', yerr)
                    first = vs[ssd][f'rq-{metric}{appendix}'][0]
                    diff = [y - first for y in vs[ssd][f'rq-{metric}{appendix}']]
                    diffp = [(y - first) / first for y in vs[ssd][f'rq-{metric}{appendix}']]
                    rq_scaling_plot(diff, target, ssd, labilify_metric(metric) + " delta", 20, f'diff-{metric}{appendix}', yerr)
                    rq_scaling_plot(diffp, target, ssd, labilify_metric(metric) + " incr", 20, f'diffp-{metric}{appendix}')

                # QD size plot
                if not ('rate' in ssd and len(appendix) > 1):
                    if 'e' in metric and len(metric) == 1:
                        qd_scaling_plot(vs[ssd][f'qd-b{appendix}'], target, ssd, "Bandwidth (GiB/s)", 4, f'bw{appendix}')
                        first = vs[ssd][f'qd-b{appendix}'][0]
                        diff = [y - first for y in vs[ssd][f'qd-b{appendix}']]
                        qd_scaling_plot(diff, target, ssd, "Bandwidth (GiB/s) delta", 4, f'diff-bw{appendix}')
                        diffp = trendline(vs[ssd][f'qd-b{appendix}'])
                        qd_scaling_plot(diffp, target, ssd, "Bandwidth (GiB/s) per inc", 10, f'diffp-bw{appendix}')
                    yerr = vs[ssd][f'qd-pdev-cpu{appendix}'] if 'p-cpu' in metric else None
                    qd_scaling_plot(vs[ssd][f'qd-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', yerr)
                    first = vs[ssd][f'qd-{metric}{appendix}'][0]
                    diff = [y - first for y in vs[ssd][f'qd-{metric}{appendix}']]
                    diffp = trendline(vs[ssd][f'qd-{metric}{appendix}'])
                    qd_scaling_plot(diff, target, ssd, labilify_metric(metric) + " delta", 20, f'diff-{metric}{appendix}', yerr)
                    qd_scaling_plot(diffp, target, ssd, labilify_metric(metric) + " incr", 10, f'diffp-{metric}{appendix}')

                    if not 'rate' in ssd and 'ps0' in vs[ssd]:
                        fig, ax = plt.subplots()
                        colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                        for ps in ['ps0', 'ps1']:
                            plt.plot(range(1, len(vs[ssd][ps][f'qd-b']) + 1), vs[ssd][ps][f'qd-{metric}'], color=colors[1 + int(ps[-1])], linewidth=5, label=bold(f"qd-{ps}"), marker='o', markersize=8)

                        for ps in ['ps0', 'ps1']:
                            plt.plot(range(1, len(vs[ssd][ps][f'rq-{metric}']) + 1), vs[ssd][ps][f'rq-{metric}'], color=colors[3 + int(ps[-1])], linewidth=5, label=bold(f"rq-{ps}"), marker='o', markersize=8)

                        if 'e' in metric and len(metric) == 1:
                            ax.set_ylabel(bold("Efficiency (MiB/J)"))
                            if target == "ssd":
                                plt.ylim(0, 400)
                            elif target == 'system':
                                plt.ylim(0, 20)
                            else:
                                plt.ylim(0, 40)
                        elif 'p-' in metric:
                            ax.set_ylabel(bold("Power (W)"))
                            if target == "ssd":
                                plt.ylim(0, 20)
                            else:
                                plt.ylim(0, 300)
                        elif 'l-' in metric:
                            ax.set_ylabel(bold("Load (cores)"))
                            plt.ylim(0, 40)

                        plt.xlim(0, 10)
                        plt.grid()
                        plt.legend()

                        ax.set_xlabel(bold("Queue depth"))
                        ax.set_xticks(range(1, len(vs[ssd][f'qd-b{appendix}']) + 1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                        save_iiswc_fig(fig, f'./plots/fig/{target}/PS-fig-QD-{target}-{ssd}-{metric}')

                # thread size plot
                if not 'rate' in ssd and len(appendix) < 2:
                    if 'e' in metric and len(metric) == 1:
                        t_scaling_plot(vs[ssd][f't-b{appendix}'], target, ssd, "Bandwidth (GiB/s)", 4, f'bw{appendix}')
                        first = vs[ssd][f't-b{appendix}'][0]
                        diff = [y - first for y in vs[ssd][f't-b{appendix}']]
                        t_scaling_plot(diff, target, ssd, "Bandwidth (GiB/s) delta", 4, f'diff-bw{appendix}')
                        diffp = [(y - first) / first for y in vs[ssd][f't-b{appendix}']]
                        t_scaling_plot(diffp, target, ssd, "Bandwidth (GiB/s) per inc", 4, f'diffp-bw{appendix}')
                    yerr = vs[ssd][f't-pdev-cpu{appendix}'] if 'p-cpu' in metric else None
                    t_scaling_plot(vs[ssd][f't-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', yerr)
                    first = vs[ssd][f't-{metric}{appendix}'][0]
                    diff = [y - first for y in vs[ssd][f't-{metric}{appendix}']]
                    diffp = [(y - first) / first for y in vs[ssd][f't-{metric}{appendix}']]
                    t_scaling_plot(diff, target, ssd, labilify_metric(metric) + " delta", 60, f'diff-{metric}{appendix}', yerr)
                    t_scaling_plot(diffp, target, ssd, labilify_metric(metric) + " incr", 20, f'diffp-{metric}{appendix}')

                # RQD size plot
                if not ('rate' in ssd) and not (ssd == "ssd_c" or ssd == "ssd_a") and not (len(appendix) > 1):
                    fig, ax = plt.subplots()
                    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                    plt.plot(range(1, len(vs[ssd][f'rqd-b{appendix}']) + 1), vs[ssd][f'rqd-{metric}{appendix}'], color=colors[1], linewidth=5, label=bold("Request size"), marker='o', markersize=8)

                    if 'e' in metric and len(metric) == 1:
                        ax.set_ylabel(bold("Efficiency (MiB/J)"))
                        if target == "ssd":
                            plt.ylim(0, 400)
                        elif target == 'system':
                            plt.ylim(0, 20)
                        else:
                            plt.ylim(0, 40)
                    elif 'p-' in metric:
                        ax.set_ylabel(bold("Power (W)"))
                        if target == "ssd":
                            plt.ylim(0, 20)
                        else:
                            plt.ylim(0, 300)
                    elif 'l-' in metric:
                        ax.set_ylabel(bold("Load (cores)"))
                        plt.ylim(0, 40)

                    plt.xlim(0, 10)
                    plt.grid()
                    plt.legend()

                    ax.set_xlabel(bold("Queue depth"))
                    ax.set_xticks(range(1, len(vs[ssd][f'rqd-b{appendix}']) + 1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                    save_iiswc_fig(fig, f'./plots/fig/{target}/RATE-fig-QD-{target}-{ssd}-{metric}{appendix}')

                    if 'e' in metric and len(metric) == 1:
                        fig, ax = plt.subplots()
                        colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                        plt.plot(range(1, len(vs[ssd][f'rqd-b{appendix}']) + 1), vs[ssd][f'rqd-b{appendix}'], color=colors[1], linewidth=5, label=bold("Request size"), marker='o', markersize=8)

                        ax.set_ylabel(bold("Bandwidth (GiB/s)"))
                        plt.ylim(0, 3)
                        plt.xlim(0, 10)
                        plt.grid()
                        plt.legend()

                        ax.set_xlabel(bold("Queue depth"))
                        ax.set_xticks(range(1, len(vs[ssd][f'rqd-b{appendix}']) + 1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                        save_iiswc_fig(fig, f'./plots/fig/{target}/RATE-fig-QDBW{appendix}-{target}-{ssd}-{metric}')

                    fig, ax = plt.subplots()
                    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                    plt.plot(range(1, len(vs[ssd][f'rrq-b{appendix}']) + 1), vs[ssd][f'rrq-{metric}{appendix}'], color=colors[1], linewidth=5, label=bold("Request size"), marker='o', markersize=8)

                    if 'e' in metric and len(metric) == 1:
                        ax.set_ylabel(bold("Efficiency (MiB/J)"))
                        if target == "ssd":
                            plt.ylim(0, 400)
                        elif target == 'system':
                            plt.ylim(0, 20)
                        else:
                            plt.ylim(0, 40)
                    elif 'p-' in metric:
                        ax.set_ylabel(bold("Power (W)"))
                        if target == "ssd":
                            plt.ylim(0, 20)
                        else:
                            plt.ylim(0, 300)
                    elif 'l-' in metric:
                        ax.set_ylabel(bold("Load (cores)"))
                        plt.ylim(0, 40)

                    plt.xlim(0, 10)
                    plt.grid()
                    plt.legend()

                    ax.set_xlabel(bold("Queue depth"))
                    ax.set_xticks(range(1, len(vs[ssd][f'rrq-b{appendix}']) + 1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                    save_iiswc_fig(fig, f'./plots/fig/{target}/RATE-fig-RQ-{target}-{ssd}-{metric}{appendix}')

                # access pattern
                if not 'rate' in ssd and len(appendix) < 2 and not 'ssd_a' in ssd:
                    fig, ax = plt.subplots()
                    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                    plt.plot(range(1, 4), [vs[ssd][f'a-rw-{metric}'], vs[ssd][f'a-sw-{metric}'], vs[ssd][f'a-randw-{metric}']], color=colors[1], linewidth=5, label=bold("Thread count"), marker='o', markersize=8)

                    if 'e' in metric and len(metric) == 1:
                        ax.set_ylabel(bold("Efficiency (MiB/J)"))
                        if target == "ssd":
                            plt.ylim(0, 400)
                        elif target == 'system':
                            plt.ylim(0, 20)
                        else:
                            plt.ylim(0, 40)
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

                    ax.set_xlabel(bold("workload"))
                    ax.set_xticks(range(1, 4), ['rw', 'sw', 'randw'])

                    save_iiswc_fig(fig, f'./plots/fig/{target}/A-fig-{target}-{ssd}-{metric}')

                # engines
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
                                    labelen = 'iou'
                                if en == 'Polling':
                                    labelen = 'iou + s'
                                elif en == 'KernelPolling':
                                    labelen = 'iou + c'
                                if 'Polling' in en and 't' in wl and 'ssd_e' in ssd:
                                    continue
                                if 'SPDK' in en and 't' in wl:
                                    continue
                                plt.plot(vs[ssd][en][sze][f'{wl}-b{appendix}'], vs[ssd][en][sze][f'{wl}-{metric}{appendix}'], color=colors[i + 1], linewidth=5, label=bold(labelen), marker='o', markersize=8)

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

                            save_iiswc_fig(fig, f'./plots/fig/{target}/engine/fig-{wl}-{sze}-bw{appendix}-correlation-{target}-{ssd}-{metric}')

                            fig, ax = plt.subplots()
                            colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                            for i, en in enumerate(['libaio', 'io_uring', 'Polling', 'KernelPolling', 'SPDK']):
                                labelen = en
                                if en == "io_uring":
                                    labelen = 'io_uring + INT'
                                if en == 'Polling':
                                    labelen = 'io_uring + spoll'
                                elif en == 'KernelPolling':
                                    labelen = 'io_uring + cpoll'
                                if 'Polling' in en and 't' in wl and 'ssd_e' in ssd:
                                    continue
                                if 'SPDK' in en and 't' in wl:
                                    continue
                                plt.plot(range(1, len(vs[ssd][en][sze][f'{wl}-b{appendix}']) + 1), vs[ssd][en][sze][f'{wl}-{metric}{appendix}'], color=colors[i + 1], linewidth=5, label=bold(labelen), marker='o', markersize=8)

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
                            ax.set_xticks(range(1, len([1, 2, 4, 8, 16, 32, 64, 128, 256]) + 1), [bold(qdi) for qdi in ['1', '2', '4', '8', '16', '32', '64', '128', '256']])

                            save_iiswc_fig(fig, f'./plots/fig/{target}/engine/fig-{wl}-{sze}-bw{appendix}-SCALING-{target}-{ssd}-{metric}')

                # DVFS
                if not 'rate' in ssd and '1ghz' in vs[ssd] and len(appendix) < 2:
                    for l1, l2, slab in [(0, 8, '4k'), (9, 17, '1m')]:
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(vs[ssd][dvfs][f'iodepth-b{appendix}'][l1:l2], vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
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

                        save_iiswc_fig(fig, f'./plots/fig/{target}/DVFS-{slab}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}')

                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(range(1, len(vs[ssd][dvfs][f'iodepth-b{appendix}'][l1:l2 + 1]) + 1), vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][l1:l2 + 1], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
                        if '4k' in slab:
                            plt.plot(range(1, len(vs[ssd][f'qd-b{appendix}']) + 1), vs[ssd][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('Default'), marker='o', markersize=8)
                        elif '1m' in slab and 'ssd_d' in ssd:
                            plt.plot(range(1, len(vs[ssd]['io_uring']['1m'][f'qd-b{appendix}']) + 1), vs[ssd]['io_uring']['1m'][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('Default'), marker='o', markersize=8)

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

                        plt.xlim(0, 10)
                        plt.grid()
                        if '4k' in slab:
                            plt.legend(ncol=2)

                        ax.set_xlabel(bold("Queue depth"))
                        ax.set_xticks(range(1, 10), [bold(qdi) for qdi in ['1', '2', '4', '8', '16', '32', '64', '128', '256']])

                        save_iiswc_fig(fig, f'./plots/fig/{target}/DVFS-QD-{slab}-fig-bw{appendix}-{target}-{ssd}-{metric}')

                    for l1, l2, slab in [(0, 8, '4k'), (9, 17, '1m')]:
                        if 'ssd_d' in ssd:
                            break
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(vs[ssd][dvfs][f'threads-b{appendix}'][l1:l2], vs[ssd][dvfs][f'threads-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
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

                        save_iiswc_fig(fig, f'./plots/fig/{target}/DVFST-{slab}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}')

                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(range(1, len(vs[ssd][dvfs][f'threads-b{appendix}'][l1:l2]) + 1), vs[ssd][dvfs][f'threads-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
                        if '4k' in slab:
                            plt.plot(range(1, len(vs[ssd][f't-b{appendix}']) + 1), vs[ssd][f't-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('None'), marker='o', markersize=8)

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

                        save_iiswc_fig(fig, f'./plots/fig/{target}/DVFST-T-{slab}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}')
