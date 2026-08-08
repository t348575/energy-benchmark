import json
import matplotlib.pyplot as plt

import os
import subprocess

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

def set_font(size):
    text_font_size = size
    marker_font_size = size
    label_font_size = size
    axes_font_size = size

    plt.rc('pdf', fonttype=42)
    plt.rc('ps', fonttype=42)
    plt.rc('font', size=text_font_size, weight="bold", family='serif', serif=['DejaVu Serif'])
    plt.rc('axes', labelsize=axes_font_size,labelweight="bold")
    plt.rc('xtick', labelsize=label_font_size)
    plt.rc('ytick', labelsize=label_font_size)
    plt.rc('legend', fontsize=label_font_size)

    plt.rcParams['text.usetex'] = False
    plt.rcParams['figure.figsize'] = (8.5, 6.5)

def set_standard_font():
    set_font(21)

def bold(text):
    return str(text)

def savefig(fig, filename):
    dirname = os.path.dirname(filename)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    fig.savefig(filename, bbox_inches="tight")

GREEN = "#117733"
TEAL  = "#44AA99"
CYAN = "#88CCEE"
OLIVE = "#999933"
SAND = "#DDCC77"
ROSE   = "#CC6677"
BLUE = "#88CCEE"
MAGENTA = "#AA4499"
GREY = GRAY = "#DDDDDD"

vs={}
vs['ssd_c']={}; vs['ssd_c_rate']={}
vs['ssd_d']={}; vs['ssd_d_rate']={}
vs['ssd_e']={}; vs['ssd_e_rate']={}
vs['ssd_b']={}; vs['ssd_b_rate']={}

qd_ssd_c="qd-2025-11-06_14-48-52"
qd_ssd_d="qd-2025-11-13_03-53-55"
qd_ssd_e="qd-2025-11-16_20-06-30"
qd_ssd_b="qd-2025-12-02_02-42-57"

rq_ssd_c="request-size-2025-11-06_16-17-20"
rq_ssd_d="request-size-2025-11-12_09-32-33"
rq_ssd_e="request-size-2025-11-16_20-36-10"
rq_ssd_b="request-size-2025-12-02_03-26-10"

a_ssd_c="access-patterns-2025-11-01_19-25-55"
a_ssd_d="access-patterns-2025-11-09_19-42-23"
a_ssd_e="access-patterns-2025-11-18_00-31-53"
a_ssd_b="access-patterns-2025-11-18_00-31-53"

qd_rate_ssd_c="qd-rate-limited-2025-11-06_15-57-05"
qd_rate_ssd_d="qd-rate-limited-2025-11-10_16-03-45"
qd_rate_ssd_e="qd-req-size-rate-limited-2025-11-24_12-57-43"

pst_ssd_c="nvme-pst-eff-2025-11-16_18-44-20"; ssd_c_psts=2
#pst_ssd_d="nvme-pst-eff-2025-11-18_05-43-14"
pst_ssd_e="nvme-pst-eff-2025-11-18_12-32-49"; ssd_e_psts=2
pst_ssd_b="nvme-pst-eff-2025-12-02_00-57-09"; ssd_b_psts=3

psts_ran = {
    'ssd_c' : ssd_c_psts,
    'ssd_e' : ssd_e_psts,
    'ssd_b': ssd_b_psts
}

maxtransfer = {
    'ssd_e': (6, '128'),
    'ssd_d': (10, '2048'),
    'ssd_c':  (9, '1024'),
    'ssd_b':  (7, '256')
}

generated_plot_data = set()

def open_json(filename):
    file_path = filename if os.path.isabs(filename) else os.path.join(BASE_DIR, filename)
    if not os.path.exists(file_path):
        parts = os.path.normpath(filename).split(os.sep)
        if 'results' in parts and 'plot_data' in parts:
            results_index = parts.index('results')
            if len(parts) > results_index + 1:
                result_dir = os.path.join('results', parts[results_index + 1])
                result_path = os.path.join(BASE_DIR, result_dir)
                if os.path.isdir(result_path) and result_dir not in generated_plot_data:
                    generated_plot_data.add(result_dir)
                    print(f'{filename} missing; regenerating plot data for {result_dir}')
                    env = os.environ.copy()
                    env['SKIP_PLOT'] = '1'
                    subprocess.run(
                        [
                            'cargo', 'r', '--release', '-p', 'nvme-energy-bench',
                            '--', 'plot', '-f', f'../nvme-energy-bench-paper-plots/{result_dir}',
                        ],
                        cwd='/home/joefe/repos/energy-benchmark',
                        env=env,
                        check=True,
                    )
    return open(filename)

def parse_bar(filename, ps=0):
    with open_json(filename) as f:
        j = json.load(f)
        return j['data'][ps]

def parse_bars(filenames, innerind=-1):
    out = []
    for filename in filenames:
        with open_json(filename) as f:
            j = json.load(f)
            for o in j['data'][0]:
                out.append(o)
    return out


def find_inner_indexe(filenames, label_to_grep):
    for filename in filenames:
        with open_json(filename) as f:
            j = json.load(f)

            comeon = zip(j['data'][0], j['labels'])
            for number, lab in comeon:
                if label_to_grep in lab:
                    return number
                    break 
    return -1

def parse_bars_inner(filenames, label_to_grep):
    out = []
    for filename in filenames:
        with open_json(filename) as f:
            j = json.load(f)

            comeon = zip(j['data'][0], j['labels'])
            for number, lab in comeon:
                if label_to_grep in lab:
                    out.append(number)
                    break 
            #out.append(j['data'][0][innerind])
    return out

def parse_efficiency(filenames, ps=0):
    out = []
    for filename in filenames:
        with open_json(filename) as f:
            j = json.load(f)
            for x in j:
                out.append(x[ps])
    return out

def parse_bw(filename):
    o = 0
    with open_json(filename) as f:
        j = json.load(f)
        o = sum([job['read']['bw_mean'] / (2**20) for job in j['jobs']]) + sum([job['write']['bw_mean'] / (2**20) for job in j['jobs']])
    #print(filename, o * 1024)
    #if '03-53-55' in filename and 'i0-8' in filename:
    #    exit(1)
    return o

def trendline(arr):
    o = [1]
    for i in range(1, len(arr)):
        o.append(arr[i] / arr[i-1])
    print(o)
    return o

max_rqs = {
 'ssd_c': 0,
 'ssd_d': 0,
 'ssd_e': 0,
 'ssd_e_rate': 0,   
 'ssd_b': 0,   
 'ssd_b_rate': 0   
}
def verify_max_rq(filename, ssd):
    o = 0
    with open_json(filename) as f:
        j = json.load(f)
        ut = j['disk_util'][0]
        rq_size =  ut['read_sectors'] / ut['read_ios']
        print(ssd, rq_size/2, filename)
        if rq_size > max_rqs[ssd]:
            max_rqs[ssd] = rq_size
    return o

def parse_lat(filename):
    with open_json(filename) as f:
        j = json.load(f)
        return j['jobs'][0]['read']['clat_ns']['percentile']['95.000000']

def parse_bw_dev(filename):
    with open_json(filename) as f:
        j = json.load(f)
        return (sum([job['read']['bw_dev']**2 for job in j['jobs']]) / len(j['jobs'])) ** 0.5

# really joseph
def parametrize_result_dir(prefix, suffix, y=False, ran=None):
    if y:
        return [f'{prefix}-{i}-{suffix}' for i in ran]
    else:
        return [f'{prefix}-{suffix}']

def reswaparray(a):
    # this is really weird behavior of energy bench
    l = a[::2]
    r = a[1::2]
    return l + r

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
        print(i)
        plt.plot(bwar, metricar, color=colors[i], linewidth=8, marker='o', markersize=12, label=bold(llab))
        if berr:
            plt.errorbar(bwar, metricar, yerr = [abs(y) for y in berr], fmt ='o', color=colors[i], ecolor=colors[i], elinewidth=4, linewidth=12)
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

    filename = f'./plots/ioshaping/bw/{target}/{ssd}-{label}.pdf'
    if 'ssd_b' in filename:
        if 'ssd' in filename and '-e' in filename:
            plt.legend(fontsize=28, columnspacing=-0.1, loc=(0.25, 0.01), handletextpad=0.1)
        elif 'cpu' in filename and '-e' in filename:
            plt.legend(fontsize=28, columnspacing=-0.1, loc=(0.01, 0.52))
        else:
            plt.legend(fontsize=28, columnspacing=-0.1)
    


    print(filename)
    if 'ssd_e' in filename and 'absolute-p' in filename:
        print('hello',lines)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    savefig(fig, filename)
    set_font(21)
    plt.close(fig)

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
    print(filename)
    savefig(fig, filename)
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
    savefig(fig, filename)
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
        savefig(fig, filename)
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

set_standard_font()

for target, suffix in [('cpu', 'only-cpu-bytes'), ('ssd', 'bytes'), ('both', '+cpu-bytes'), ('system', None)]:

    # QD
    for access, qappendix in [('', ''), ('seqread-', '-seq')]:
        for label, qd in [('ssd_b', qd_ssd_b), ('ssd_c', qd_ssd_c), ('ssd_d', qd_ssd_d), ('ssd_e', qd_ssd_e), ('ssd_c_rate', qd_rate_ssd_c), ('ssd_d_rate', qd_rate_ssd_d), ('ssd_e_rate', qd_rate_ssd_e)]:
            parame = 'rate' in label
            
            if len(access) > 1 and 'rate' in label:
                continue
            if not 'system' in target:
                vs[label][f'qd-e{qappendix}'] = parse_efficiency(parametrize_result_dir(f"results/{qd}/plots/efficiency/plot_data/{access}iodepth", f"{suffix}-j.json", parame,[2**j for j in range(0, 9)]))
            try:
                vs[label][f'qd-b{qappendix}'] = [parse_bw(f"results/{qd}/data/{access}iodepth-ps0-i0-{i}/results.json") for i in range(0, 9)]
            except:
                vs[label][f'qd-b{qappendix}'] = [parse_bw(f"results/{qd}/data/{access}iodepth-{2**i}-ps0-i0-0/results.json") for i in range(0, 9)]
            if 'ssd' in target:
                # power SSD
                vs[label][f'qd-p-ssd{qappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "ssd.bar.json", parame, [2**j for j in range(0, 9)]))
                # stdev power SSD
                vs[label][f'qd-pdev-ssd{qappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "stdev-ssd.bar.json", parame, [2**j for j in range(0, 9)]))
            elif 'cpu' in target:
                # power CPU
                vs[label][f'qd-p-cpu{qappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "cpu.bar.json", parame, [2**j for j in range(0, 9)]))
                # load cpu
                vs[label][f'qd-l-cpu{qappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "load.bar.json", parame, [2**j for j in range(0, 9)]))
                # stdev
                vs[label][f'qd-pdev-cpu{qappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "stdev-cpu.bar.json", parame, [2**j for j in range(0, 9)]))
            elif 'both' in target:
                # power SSD
                ssd = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "ssd.bar.json", parame, [2**j for j in range(0, 9)]))
                # power CPU
                cpu = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "cpu.bar.json", parame, [2**j for j in range(0, 9)]))
                vs[label][f'qd-p-both{qappendix}'] = [x + y for x, y in zip(ssd, cpu)]
            elif 'system' in target:
                system = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}iodepth", "system.bar.json", parame, [2**j for j in range(0, 9)]))
                vs[label][f'qd-p-system{qappendix}'] = system
                #print(system, vs[label]['qd-b'])
                vs[label][f'qd-e{qappendix}'] = [ (x*1024)/y for x,y in zip(vs[label][f'qd-b{qappendix}'], system)]

    # access pattern
    for pattern, patternsuffix in [('rw', 'rand-write'), ('sw', 'seq-write'), ('randw', 'read-write')]:
        for label, qd in [('ssd_c', a_ssd_c), ('ssd_d', a_ssd_d), ('ssd_e', a_ssd_e)]:
            appendix = qappendix = ''
            if not 'system' in target:
                vs[label][f'a-{pattern}-e'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/{patternsuffix}-{suffix}-j.json"])
            vs[label][f'a-b{pattern}'] = [parse_bw(f"results/{qd}/data/{patternsuffix}-ps0-i0-0/results.json")]
            if 'ssd' in target:
                # power SSD
                vs[label][f'a-{pattern}-p-ssd'] = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-ssd.bar.json")
            elif 'cpu' in target:
                # power CPU
                vs[label][f'a-{pattern}-p-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-cpu.bar.json")
                # load cpu
                vs[label][f'a-{pattern}-l-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-load.bar.json")
                # stdev
                vs[label][f'a-{pattern}-pdev-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-stdev-cpu.bar.json")
            elif 'both' in target:
                # power SSD
                ssd = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-ssd.bar.json")
                # power CPU
                cpu = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-cpu.bar.json")
                vs[label][f'a-{pattern}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
            elif 'system' in target:
                system = parse_bar(f"results/{qd}/plots/power/plot_data/{patternsuffix}-system.bar.json")
                vs[label][f'a-{pattern}-p-system'] = system
                #print(system, vs[label]['qd-b'])
                vs[label][f'a-{pattern}-e'] = [ (x*1024)/y for x,y in zip(vs[label][f'a-b{pattern}'], system)]

    # rate eee
    for label, qd in [('ssd_d', qd_ssd_d), ('ssd_e', qd_ssd_e)]:
        appendix = qappendix = ''
        if not 'system' in target:
            vs[label][f'rqd-e{qappendix}'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/read-limited-{suffix}-j.json"])
        vs[label][f'rqd-b{qappendix}'] = [parse_bw(f"results/{qd}/data/read-limited-ps0-i0-{i}/results.json") for i in range(0, 9)]
        if 'ssd' in target:
            # power SSD
            vs[label][f'rqd-p-ssd{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-ssd.bar.json")
        elif 'cpu' in target:
            # power CPU
            vs[label][f'rqd-p-cpu{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-cpu.bar.json")
            # load cpu
            vs[label][f'rqd-l-cpu{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-load.bar.json")
            # stdev
            vs[label][f'rqd-pdev-cpu{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-stdev-cpu.bar.json")
        elif 'both' in target:
            # power SSD
            ssd = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-ssd.bar.json")
            # power CPU
            cpu = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-cpu.bar.json")
            vs[label][f'rqd-p-both{qappendix}'] = [x + y for x, y in zip(ssd, cpu)]
        elif 'system' in target:
            system = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-system.bar.json")
            vs[label][f'rqd-p-system{qappendix}'] = system
            #print(system, vs[label]['qd-b'])
            vs[label][f'rqd-e{qappendix}'] = [ (x*1024)/y for x,y in zip(vs[label][f'rqd-b{qappendix}'], system)]

    for label, qd in [('ssd_d', rq_ssd_d), ('ssd_e', rq_ssd_e)]:
        appendix = qappendix = ''
        if not 'system' in target:
            vs[label][f'rrq-e{qappendix}'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/read-limited-{suffix}-j.json"])
        vs[label][f'rrq-b{qappendix}'] = [parse_bw(f"results/{qd}/data/read-limited-ps0-i0-{i}/results.json") for i in range(0, 9)]
        if 'ssd' in target:
            # power SSD
            vs[label][f'rrq-p-ssd{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-ssd.bar.json")
        elif 'cpu' in target:
            # power CPU
            vs[label][f'rrq-p-cpu{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-cpu.bar.json")
            # load cpu
            vs[label][f'rrq-l-cpu{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-load.bar.json")
            # stdev
            vs[label][f'rrq-pdev-cpu{qappendix}'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-stdev-cpu.bar.json")
        elif 'both' in target:
            # power SSD
            ssd = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-ssd.bar.json")
            # power CPU
            cpu = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-cpu.bar.json")
            vs[label][f'rrq-p-both{qappendix}'] = [x + y for x, y in zip(ssd, cpu)]
        elif 'system' in target:
            system = parse_bar(f"results/{qd}/plots/power/plot_data/read-limited-system.bar.json")
            vs[label][f'rrq-p-system{qappendix}'] = system
            #print(system, vs[label]['qd-b'])
            vs[label][f'rrq-e{qappendix}'] = [ (x*1024)/y for x,y in zip(vs[label][f'rrq-b{qappendix}'], system)]

    # threads
    for policy, tappendix in [("singlecore", "singlecore"), ("round-robin", "round-robin"), ("randread-threads", ""), ("seqread-threads", "seqread-threads")]:
        for label, qd in [('ssd_b', qd_ssd_b), ('ssd_c', qd_ssd_c), ('ssd_d', qd_ssd_d), ('ssd_e', qd_ssd_e), ('ssd_c_rate', qd_rate_ssd_c), ('ssd_d_rate', qd_rate_ssd_d), ('ssd_e_rate', qd_rate_ssd_e)]:
            parame = 'rate' in label
            if policy != "randread-threads" and label != 'ssd_c':
                continue

            # WHYYYYY do you name dirs differently
            read = policy if not parame else "threads"
            if not 'system' in target:
                vs[label][f't{tappendix}-e'] = parse_efficiency(parametrize_result_dir(f"results/{qd}/plots/efficiency/plot_data/{read}", f"{suffix}-j.json", parame,[2**j for j in range(0, 9)]))
            try:
                vs[label][f't{tappendix}-b'] = [parse_bw(f"results/{qd}/data/{read}-ps0-i0-{i}/results.json") for i in range(0, 9)]
                vs[label][f't{tappendix}-bdev'] = [parse_bw_dev(f"results/{qd}/data/{read}-ps0-i0-{i}/results.json") for i in range(0, 9)]
            except:
                vs[label][f't{tappendix}-b'] = [parse_bw(f"results/{qd}/data/{read}-{2**i}-ps0-i0-0/results.json") for i in range(0, 9)]
                vs[label][f't{tappendix}-bdev'] = [parse_bw_dev(f"results/{qd}/data/{read}-{2**i}-ps0-i0-0/results.json") for i in range(0, 9)]
            if 'ssd' in target:
                # power SSD
                vs[label][f't{tappendix}-p-ssd'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "ssd.bar.json", parame, [2**j for j in range(0, 9)]))
                # stdev power SSD
                vs[label][f't{tappendix}-pdev-ssd'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "stdev-ssd.bar.json", parame, [2**j for j in range(0, 9)]))
            elif 'cpu' in target:
                # power CPU
                vs[label][f't{tappendix}-p-cpu'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "cpu.bar.json", parame, [2**j for j in range(0, 9)]))
                # load cpu
                vs[label][f't{tappendix}-l-cpu'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "load.bar.json", parame, [2**j for j in range(0, 9)]))
                # stdev
                vs[label][f't{tappendix}-pdev-cpu'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "stdev-cpu.bar.json", parame, [2**j for j in range(0, 9)]))
            elif 'both' in target:
                # power SSD
                ssd = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "ssd.bar.json", parame, [2**j for j in range(0, 9)]))
                # power CPU
                cpu = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "cpu.bar.json", parame, [2**j for j in range(0, 9)]))
                vs[label][f't{tappendix}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
            elif 'system' in target:
                system = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{read}", "system.bar.json", parame, [2**j for j in range(0, 9)]))
                vs[label][f't{tappendix}-p-system'] = system
                vs[label][f't{tappendix}-e'] = [ (x*1024)/y for x,y in zip(vs[label][f't{tappendix}-b'], system)]

    # RQ
    for access, rqappendix in [('read', ''), ('seqread', '-seq'), ('rq', '')]:
        for label, qd in [('ssd_b', rq_ssd_b), ('ssd_c', rq_ssd_c), ('ssd_d', rq_ssd_d), ('ssd_e', rq_ssd_e), ('ssd_e_rate', qd_rate_ssd_e)]:
            parame = 'rate' in label
            if (parame and not 'rq' in access) or (not parame and 'rq' in access):
                continue
            if not 'system' in target:
                vs[label][f'rq-e{rqappendix}'] = parse_efficiency(parametrize_result_dir(f"results/{qd}/plots/efficiency/plot_data/{access}", f"{suffix}-j.json", parame,[2**j for j in range(0, 9)]))
            try:
                vs[label][f'rq-b{rqappendix}'] =  [parse_bw(f"results/{qd}/data/{access}-ps0-i0-{i}/results.json") for i in range(11)]
                [verify_max_rq(f"results/{qd}/data/{access}-ps0-i0-{i}/results.json", label) for i in range(11)]
            except:
                vs[label][f'rq-b{rqappendix}'] =  [parse_bw(f"results/{qd}/data/{access}-{2**i}-ps0-i0-0/results.json") for i in range(9)]
                [verify_max_rq(f"results/{qd}/data/{access}-{2**i}-ps0-i0-0/results.json", label) for i in range(9)]
            if 'ssd' in target:
                # power SSD
                vs[label][f'rq-p-ssd{rqappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "ssd.bar.json", parame, [2**j for j in range(0, 9)]))
                # stdev
                vs[label][f'rq-pdev-ssd{rqappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "stdev-ssd.bar.json", parame, [2**j for j in range(0, 9)]))
            elif 'cpu' in target:
                # power CPU
                vs[label][f'rq-p-cpu{rqappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "cpu.bar.json", parame, [2**j for j in range(0, 9)]))
                # load cpu
                vs[label][f'rq-l-cpu{rqappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "load.bar.json", parame, [2**j for j in range(0, 9)]))
                # stdev
                vs[label][f'rq-pdev-cpu{rqappendix}'] = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "stdev-cpu.bar.json", parame, [2**j for j in range(0, 9)]))
            elif 'both' in target:
                # power SSD
                ssd = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "ssd.bar.json", parame, [2**j for j in range(0, 9)]))
                # power CPU
                cpu = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "cpu.bar.json", parame, [2**j for j in range(0, 9)]))
                vs[label][f'rq-p-both{rqappendix}'] = [x + y for x, y in zip(ssd, cpu)]
            elif 'system' in target:
                system = parse_bars(parametrize_result_dir(f"results/{qd}/plots/power/plot_data/{access}", "system.bar.json", parame, [2**j for j in range(0, 9)]))
                vs[label][f'rq-p-system{rqappendix}'] = system
                vs[label][f'rq-e{rqappendix}'] = [ (x*1024)/y for x,y in zip(vs[label][f'rq-b{rqappendix}'], system)]
              
        # DVFS 
        for label, qds in [\
                ('ssd_e', \
                    ['dvfs-1ghz-2025-11-16_10-59-27', \
                     'dvfs-1.44ghz-2025-11-16_11-56-59', \
                     'dvfs-1.88ghz-2025-11-16_12-25-51', \
                     'dvfs-2.32ghz-2025-11-16_12-54-36', \
                     'dvfs-2.76ghz-2025-11-16_13-23-16', \
                     'dvfs-3.2ghz-2025-11-16_11-28-29']),\
                ('ssd_b', \
                    ['dvfs-1ghz-2025-12-01_22-02-14', \
                     'dvfs-1.44ghz-2025-12-01_21-20-54', \
                     'dvfs-1.88ghz-2025-12-01_20-39-44', \
                     'dvfs-2.32ghz-2025-12-01_19-58-41', \
                     'dvfs-2.76ghz-2025-12-01_19-17-49', \
                     'dvfs-3.2ghz-2025-12-01_18-36-58']),\
                ('ssd_d', \
                    ['dvfs-1ghz-2025-11-13_00-32-26', \
                     'dvfs-1.44ghz-2025-11-13_01-15-38', \
                     'dvfs-1.88ghz-2025-11-13_01-44-19', \
                     'dvfs-2.32ghz-2025-11-13_02-12-56', \
                     'dvfs-2.76ghz-2025-11-13_02-41-28', \
                     'dvfs-3.2ghz-2025-11-13_11-09-54'])
            ]:
            for qd in qds:
                hz=qd.split('-')[1]
                vs[label][hz] = {}
                for access in ['iodepth', 'threads']:
                    if not 'system' in target:
                        vs[label][hz][f'{access}-e'] = reswaparray(parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/{access}-{suffix}-j.json"]))
                    vs[label][hz][f'{access}-b'] = [parse_bw(f"results/{qd}/data/{access}-ps0-i0-{i}/results.json") for i in range(0, 18)]
                    if 'ssd' in target:
                        # power SSD
                        vs[label][hz][f'{access}-p-ssd'] = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ssd.bar.json"))
                    elif 'cpu' in target:
                        # power CPU
                        vs[label][hz][f'{access}-p-cpu'] = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-cpu.bar.json"))
                        # load cpu
                        vs[label][hz][f'{access}-l-cpu'] = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-load.bar.json"))
                        # stdev
                        vs[label][hz][f'{access}-pdev-cpu'] = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-stdev-cpu.bar.json"))
                    elif 'both' in target:
                        # power SSD
                        ssd = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ssd.bar.json"))
                        # power CPU
                        cpu = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-cpu.bar.json"))
                        vs[label][hz][f'{access}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
                    elif 'system' in target:
                        system = reswaparray(parse_bar(f"results/{qd}/plots/power/plot_data/{access}-system.bar.json"))
                        vs[label][hz][f'{access}-p-system'] = system
                        vs[label][hz][f'{access}-e'] = [ (x*1024)/y for x,y in zip(vs[label][hz][f'{access}-b'], system)]

        # DVFS engine 
        """
        for label, qds in [\
                ('ssd_b', \
                    ['dvfs-1ghz-2025-12-01_22-02-14', \
                     'dvfs-1.44ghz-fio-engines-2025-12-02_16-46-17', \
                     'dvfs-1.88ghz-fio-engines-2025-12-02_16-27-02', \
                     'dvfs-2.32ghz-fio-engines-2025-12-02_16-07-48', \
                     'dvfs-2.76ghz-fio-engines-2025-12-02_15-48-40', \
                     'dvfs-3.2ghz-fio-engines-2025-12-02_15-29-23']),\
                ('ssd_d', \
                    ['dvfs-1ghz-2025-12-05_15-40-36', \
                     'dvfs-1.44ghz-2025-12-05_15-21-12', \
                     'dvfs-1.88ghz-2025-12-05_15-02-01', \
                     'dvfs-2.32ghz-2025-12-05_14-42-53', \
                     'dvfs-2.76ghz-2025-12-05_14-23-49', \
                     'dvfs-3.2ghz-2025-12-05_14-04-40'])
            ]:
            # if 'enhz' not in vs[label]:
            #     vs[label]['enhz'] = {}
            for qd in qds:
                hz=qd.split('-')[1]
                # vs[label]['enhz'][hz] = {}
                for access in ['iodepth']:
                    if not 'system' in target:
                        # vs[label]['enhz'][hz][f'{access}-e'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/{access}-ioengines-{suffix}-j.json"])
                    # vs[label]['enhz'][hz][f'{access}-b'] = [parse_bw(f"results/{qd}/data/{access}-ioengines-ps0-i0-{i}/results.json") for i in range(0, 12)]
                    if 'ssd' in target:
                        # power SSD
                        # vs[label]['enhz'][hz][f'{access}-p-ssd'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-ssd.bar.json")
                    elif 'cpu' in target:
                        # power CPU
                        # vs[label]['enhz'][hz][f'{access}-p-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-cpu.bar.json")
                        # load cpu
                        # vs[label]['enhz'][hz][f'{access}-l-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-load.bar.json")
                        # stdev
                        # vs[label]['enhz'][hz][f'{access}-pdev-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-stdev-cpu.bar.json")
                    elif 'both' in target:
                        # power SSD
                        ssd = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-ssd.bar.json")
                        # power CPU
                        cpu = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-cpu.bar.json")
                        # if 'enhz' in vs[label]:
                        #     vs[label]['enhz'][hz][f'{access}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
                    elif 'system' in target:
                        system = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-system.bar.json")
                        # vs[label]['enhz'][hz][f'{access}-p-system'] = system
                        # vs[label]['enhz'][hz][f'{access}-e'] = [ (x*1024)/y for x,y in zip(vs[label][hz][f'{access}-b'], system)]
        """

        for label, qds in [\
                ('ssd_b', ['dvfs-1ghz-spdk-2025-12-02_15-22-31', 'dvfs-1.44ghz-spdk-2025-12-02_15-18-57', 'dvfs-1.88ghz-spdk-2025-12-02_15-15-26', 'dvfs-2.32ghz-spdk-2025-12-02_15-11-57', 'dvfs-2.76ghz-spdk-2025-12-02_15-08-31', 'dvfs-3.2ghz-spdk-2025-12-02_14-59-40'])\
            ]:
            if 'spdkhz' not in vs[label]:
                vs[label]['spdkhz'] = {}
            for qd in qds:
                hz=qd.split('-')[1]
                vs[label]['spdkhz'][hz] = {}
                for access in ['iodepth']:
                    if not 'system' in target:
                        vs[label]['spdkhz'][hz][f'{access}-e'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/{access}-{suffix}-j.json"])
                    vs[label]['spdkhz'][hz][f'{access}-b'] = [parse_bw(f"results/{qd}/data/{access}-ps0-i0-{i}/results.json") for i in range(0, 4)]
                    if 'ssd' in target:
                        # power SSD
                        vs[label]['spdkhz'][hz][f'{access}-p-ssd'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ssd.bar.json")
                    elif 'cpu' in target:
                        # power CPU
                        vs[label]['spdkhz'][hz][f'{access}-p-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-cpu.bar.json")
                        # load cpu
                        vs[label]['spdkhz'][hz][f'{access}-l-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-load.bar.json")
                        # stdev
                        vs[label]['spdkhz'][hz][f'{access}-pdev-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-stdev-cpu.bar.json")
                    elif 'both' in target:
                        # power SSD
                        ssd = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ssd.bar.json")
                        # power CPU
                        cpu = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-cpu.bar.json")
                        # enhz is populated by the disabled DVFS-engine block.
                        # vs[label]['enhz'][hz][f'{access}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
                    elif 'system' in target:
                        system = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-system.bar.json")
                        vs[label]['spdkhz'][hz][f'{access}-p-system'] = system
                        vs[label]['spdkhz'][hz][f'{access}-e'] = [ (x*1024)/y for x,y in zip(vs[label][hz][f'{access}-b'], system)]
    
        print(vs[label]['spdkhz'])

        # engine
        for label, qd, wl in [\
            ('ssd_e', 'io-engine-threads-2025-11-16_16-49-23', 't'),\
            ('ssd_e', 'io-engine-iodepth-2025-11-16_13-59-14', 'qd'),\
            ('ssd_d', 'io-engine-iodepth-2025-11-12_18-25-25', 'qd'),\
            ('ssd_d', 'io-engine-threads-2025-11-12_07-21-54', 't'),\
            ('ssd_b', 'io-engine-iodepth-2025-12-02_04-23-52', 'qd'),\
            ('ssd_b', 'io-engine-threads-2025-12-02_06-16-26', 't')\
            ]:
            for ei, engine in enumerate(['sync', 'posixaio', 'libaio', 'io_uring']):
                if engine not in vs[label]:
                    vs[label][engine] = {}
                for si, size in enumerate(['4k', '16k', '64k', '1m']):
                    if size not in vs[label][engine]:
                        vs[label][engine][size] = {}
                    # crazy stuff
                    jj = si * 4
                    index = jj + ei
                    #print(engine, size, index, wl)

                    greplabel = f"{engine} {size}"
                    if 'ssd_d' in label:
                        #greplabel = f"{engine}"
                        if 't' in wl and si:
                            continue
                    b = [parse_bw(f"results/{qd}/data/read-{2**i}-ps0-i0-{index}/results.json") for i in range(0, 9)]
                    vs[label][engine][size][f'{wl}-b'] = b
                    p = []
                    if 'ssd' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-ssd.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    elif 'cpu' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        vs[label][engine][size][f'{wl}-l-{target}'] = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-load.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        # stdev
                        vs[label][engine][size][f'{wl}-pdev-cpu'] = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-stdev-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    elif 'both' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-ssd.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        p = [p1 + p2 for p1, p2 in zip(p, parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel))]
                    else:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-system.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    vs[label][engine][size][f'{wl}-p-{target}'] = p
                    vs[label][engine][size][f'{wl}-e'] = [ (x*1024)/y for x,y in zip(b, p)]
        print(vs['ssd_d'][engine].keys())
        print(vs['ssd_d'][engine]['4k'].keys())
        print(vs['ssd_d']['sync']['16k'])
        print(vs['ssd_d'][engine]['64k'].keys())
        print(vs['ssd_d'][engine]['1m'].keys())
        #exit()

        for label, qd, wl in [\
            ('ssd_e', 'io-engine-iouring-iodepth-2025-11-16_15-52-10', 'qd'), \
            ('ssd_d', 'io-engine-iouring-iodepth-2025-11-12_20-30-44', 'qd'), \
            ('ssd_d', 'io-engine-iouring-threads-2025-11-12_08-49-13', 't'),\
            ('ssd_b', 'io-engine-iouring-iodepth-2025-12-02_00-00-45', 'qd') \
            ]:
            for ei, engine in enumerate(['Polling', 'KernelPolling']):
                if engine not in vs[label]:
                    vs[label][engine] = {}
                for si, size in enumerate(['4k', '16k', '64k', '1m']):
                    if size not in vs[label][engine]:
                        vs[label][engine][size] = {}
                    # crazy stuff
                    jj = si * 2
                    index = jj + ei
                    print(engine, size, index, wl)
                    # even crazier stuff. I do not get why you just remove benchmarks for no reason
                    greplabel = f"{engine} {size}"
                    if label == 'ssd_d':
                        #greplabel = f"{engine}"
                        if si > 0 and 't' in wl:
                            continue

                    b = [parse_bw(f"results/{qd}/data/read-{2**i}-ps0-i0-{index}/results.json") for i in range(0, 9)]
                    vs[label][engine][size][f'{wl}-b'] = b
                    p = []
                    if 'ssd' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-ssd.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    elif 'cpu' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        vs[label][engine][size][f'{wl}-l-{target}'] = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-load.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                         # stdev
                        vs[label][engine][size][f'{wl}-pdev-{target}'] = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-stdev-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    elif 'both' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-ssd.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        p = [p1 + p2 for p1, p2 in zip(p, parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel))]
                    else:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-system.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    vs[label][engine][size][f'{wl}-p-{target}'] = p
                    vs[label][engine][size][f'{wl}-e'] = [ (x*1024)/y for x,y in zip(b, p)]

        for label, qd, wl in [\
                ('ssd_d', 'spdk-2025-11-13_09-32-00', 'qd'),\
                ('ssd_e', 'spdk-2025-11-16_21-00-56', 'qd'),\
                ('ssd_b', 'spdk-2025-12-02_03-50-30', 'qd')\
            ]:
            for ei, engine in enumerate(['SPDK']):
                if engine not in vs[label]:
                    vs[label][engine] = {}
                for si, size in enumerate(['4k', '16k', '64k', '1m']):
                    if size not in vs[label][engine]:
                        vs[label][engine][size] = {}
                    # crazy stuff
                    index = si
                    print(engine, size, index, wl)
                    # even crazier stuff. I do not get why you just remove benchmarks for no reason
                    greplabel = f"{size}"
                    if label == 'ssd_d':
                        #greplabel = "spdk"
                        if si > 0 and 't' in wl:
                            continue

                    b = [parse_bw(f"results/{qd}/data/read-{2**i}-ps0-i0-{index}/results.json") for i in range(0, 9)]
                    vs[label][engine][size][f'{wl}-b'] = b
                    p = []
                    if 'ssd' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-ssd.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    elif 'cpu' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        vs[label][engine][size][f'{wl}-l-{target}'] = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-load.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                         # stdev
                        vs[label][engine][size][f'{wl}-pdev-{target}'] = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-stdev-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    elif 'both' in target:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-ssd.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                        p = [p1 + p2 for p1, p2 in zip(p, parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-cpu.bar.json" for i in range(0,9)], label_to_grep=greplabel))]
                    else:
                        p = parse_bars_inner([f"results/{qd}/plots/power/plot_data/read-{2**i}-system.bar.json" for i in range(0,9)], label_to_grep=greplabel)
                    vs[label][engine][size][f'{wl}-p-{target}'] = p
                    vs[label][engine][size][f'{wl}-e'] = [ (x*1024)/y for x,y in zip(b, p)]

        for label, qd, psts in [('ssd_c', pst_ssd_c, ssd_c_psts), ('ssd_e', pst_ssd_e, ssd_e_psts), ('ssd_b', pst_ssd_b, ssd_b_psts)]:
            for ps in [f'ps{i}' for i in range(0,psts)]:
                vs[label][ps] = {}
                for access in ['rq', 'qd']:
                    if not 'system' in target:
                        vs[label][ps][f'{access}-e'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/read-{access}-{suffix}-j.json"], ps=int(ps[-1]))
                    vs[label][ps][f'{access}-b'] = [parse_bw(f"results/{qd}/data/read-{access}-{ps}-i0-{i}/results.json") for i in range(0, 8)]

                    vs[label][ps]['l'] = parse_lat(f"results/{qd}/data/read-{access}-{ps}-i0-0/results.json")

                    if 'ssd' in target:
                        # power SSD
                        vs[label][ps][f'{access}-p-ssd'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-ssd.bar.json", ps=int(ps[-1]))
                    elif 'cpu' in target:
                        # power CPU
                        vs[label][ps][f'{access}-p-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-cpu.bar.json", ps=int(ps[-1]))
                        # load cpu
                        vs[label][ps][f'{access}-l-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-load.bar.json", ps=int(ps[-1]))
                        # stdev
                        vs[label][ps][f'{access}-pdev-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-stdev-cpu.bar.json", ps=int(ps[-1]))
                    elif 'both' in target:
                        # power SSD
                        ssd = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-ssd.bar.json", ps=int(ps[-1]))
                        # power CPU
                        cpu = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-cpu.bar.json", ps=int(ps[-1]))
                        vs[label][ps][f'{access}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
                    elif 'system' in target:
                        system = parse_bar(f"results/{qd}/plots/power/plot_data/read-{access}-system.bar.json", ps=int(ps[-1]))
                        vs[label][ps][f'{access}-p-system'] = system
                        vs[label][ps][f'{access}-e'] = [ (x*1024)/y for x,y in zip(vs[label][ps][f'{access}-b'], system)]

    for appendix in ['', '-seq']:
        #for ssd in ['ssd_d', 'ssd_c', 'ssd_e', 'ssd_c_rate', 'ssd_d_rate', 'ssd_e_rate']:
        #for ssd in ['ssd_b', 'ssd_d', 'ssd_e']:
        for ssd in ['ssd_d', 'ssd_b', 'ssd_e']:
            for metric in [('e'), ('p-cpu'), ('p-ssd'), ('l-cpu'), ('p-both'), ('p-system')]:
            #for metric in [('p-ssd')]:
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
                            #for ps in ['ps0', 'ps1']:
                            #    lines.append(
                            #        (vs[ssd][ps][f'rq-b'], vs[ssd][ps][f'rq-{metric}'][:8],vs[ssd][ps][f'rq-pdev-cpu'] if 'p-cpu' in metric else None, f"RQ {ps}"))
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
                        diffp = [ (y - first) / first for y in vs[ssd][f'rq-b{appendix}']] 
                        rq_scaling_plot(diffp, target, ssd, "Bandwidth (GiB/s) per inc", 4, f'diffp-bw{appendix}')
                    yerr = vs[ssd][f'rq-pdev-cpu{appendix}'] if 'p-cpu' in metric else None
                    rq_scaling_plot(vs[ssd][f'rq-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', yerr)
                    first = vs[ssd][f'rq-{metric}{appendix}'][0]
                    diff = [y - first for y in vs[ssd][f'rq-{metric}{appendix}']] 
                    diffp = [ (y - first) / first for y in vs[ssd][f'rq-{metric}{appendix}']] 
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
                        qd_scaling_plot(diffp, target, ssd
                        , "Bandwidth (GiB/s) per inc", 10, f'diffp-bw{appendix}')
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
                        
                        #print(vs[ssd]['ps0']['qd-e'], vs[ssd]['ps1']['qd-e']); exit(0)
                        for ps in ['ps0', 'ps1']:
                            plt.plot(range(1, len(vs[ssd][ps][f'qd-b'])+1), vs[ssd][ps][f'qd-{metric}'], color=colors[1+int(ps[-1])], linewidth=5, label=bold(f"qd-{ps}"), marker='o', markersize=8)

                        for ps in ['ps0', 'ps1']:
                            #print(vs[ssd][ps].keys())
                            #print(ps, ssd, vs[ssd][ps]['l'])
                            plt.plot(range(1, len(vs[ssd][ps][f'rq-{metric}'])+1), vs[ssd][ps][f'rq-{metric}'], color=colors[3+int(ps[-1])], linewidth=5, label=bold(f"rq-{ps}"), marker='o', markersize=8)

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
                        ax.set_xticks(range(1, len(vs[ssd][f'qd-b{appendix}'])+1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                        savefig(fig, f'./plots/fig/{target}/PS-fig-QD-{target}-{ssd}-{metric}.pdf')
                        plt.close(fig)      

                # thread size plot
                if not 'rate' in ssd and len(appendix) < 2:
                    if 'e' in metric and len(metric) == 1:
                        t_scaling_plot(vs[ssd][f't-b{appendix}'], target, ssd, "Bandwidth (GiB/s)", 4, f'bw{appendix}')
                        first = vs[ssd][f't-b{appendix}'][0]
                        diff = [y - first for y in vs[ssd][f't-b{appendix}']] 
                        t_scaling_plot(diff, target, ssd, "Bandwidth (GiB/s) delta", 4, f'diff-bw{appendix}')
                        diffp = [ (y - first) / first for y in vs[ssd][f't-b{appendix}']] 
                        t_scaling_plot(diffp, target, ssd, "Bandwidth (GiB/s) per inc", 4, f'diffp-bw{appendix}')
                    yerr = vs[ssd][f't-pdev-cpu{appendix}'] if 'p-cpu' in metric else None
                    t_scaling_plot(vs[ssd][f't-{metric}{appendix}'], target, ssd, labilify_metric(metric), rangify_metric(metric, target), f'{metric}{appendix}', yerr)
                    first = vs[ssd][f't-{metric}{appendix}'][0]
                    diff = [y - first for y in vs[ssd][f't-{metric}{appendix}']] 
                    diffp = [ (y - first) / first for y in vs[ssd][f't-{metric}{appendix}']] 
                    t_scaling_plot(diff, target, ssd, labilify_metric(metric) + " delta", 60, f'diff-{metric}{appendix}', yerr)
                    t_scaling_plot(diffp, target, ssd, labilify_metric(metric) + " incr", 20, f'diffp-{metric}{appendix}')
                    
                    # if ssd == 'ssd_c':
                    #     plt.plot(range(1, len(vs[ssd]['tround-robin-b'])+1), vs[ssd][f'tround-robin-{metric}'], color=colors[2], linewidth=5, label=bold("Thread rr count"), marker='o', markersize=8)
                    #     plt.plot(range(1, len(vs[ssd]['tsinglecore-b'])+1), vs[ssd][f'tsinglecore-{metric}'], color=colors[3], linewidth=5, label=bold("Thread c count"), marker='o', markersize=8)
                    #     savefig(fig, f'./plots/fig/{target}/threading-fig-T-{target}-{ssd}-{metric}.pdf')


                # RQD size plot
                if not ('rate' in ssd) and not (ssd == "ssd_c" or ssd == "ssd_b") and not (len(appendix) > 1):
                    fig, ax = plt.subplots()
                    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                    plt.plot(range(1, len(vs[ssd][f'rqd-b{appendix}'])+1), vs[ssd][f'rqd-{metric}{appendix}'], color=colors[1], linewidth=5, label=bold("Request size"), marker='o', markersize=8)

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
                    ax.set_xticks(range(1, len(vs[ssd][f'rqd-b{appendix}'])+1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                    savefig(fig, f'./plots/fig/{target}/RATE-fig-QD-{target}-{ssd}-{metric}{appendix}.pdf')
                    plt.close(fig)

                    if 'e' in metric and len(metric) == 1:
                        fig, ax = plt.subplots()
                        colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                        plt.plot(range(1, len(vs[ssd][f'rqd-b{appendix}'])+1), vs[ssd][f'rqd-b{appendix}'], color=colors[1], linewidth=5, label=bold("Request size"), marker='o', markersize=8)

                        ax.set_ylabel(bold("Bandwidth (GiB/s)"))
                        plt.ylim(0, 3)
                        plt.xlim(0, 10)
                        plt.grid()
                        plt.legend()

                        ax.set_xlabel(bold("Queue depth"))
                        ax.set_xticks(range(1, len(vs[ssd][f'rqd-b{appendix}'])+1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                        savefig(fig, f'./plots/fig/{target}/RATE-fig-QDBW{appendix}-{target}-{ssd}-{metric}.pdf')
                        plt.close(fig)

                    fig, ax = plt.subplots()
                    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                    plt.plot(range(1, len(vs[ssd][f'rrq-b{appendix}'])+1), vs[ssd][f'rrq-{metric}{appendix}'], color=colors[1], linewidth=5, label=bold("Request size"), marker='o', markersize=8)

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
                    ax.set_xticks(range(1, len(vs[ssd][f'rrq-b{appendix}'])+1), ['1', '2', '4', '8', '16', '32', '64', '128', '256'])

                    savefig(fig, f'./plots/fig/{target}/RATE-fig-RQ-{target}-{ssd}-{metric}{appendix}.pdf')
                    plt.close(fig)                 

                    # access pattern
                if not 'rate' in ssd and len(appendix) < 2 and not 'ssd_b' in ssd:
                    fig, ax = plt.subplots()
                    colors = [ROSE, CYAN, SAND, TEAL, MAGENTA]

                    plt.plot(range(1, 4), [vs[ssd][f'a-rw-{metric}'], vs[ssd][f'a-sw-{metric}'],vs[ssd][f'a-randw-{metric}']], color=colors[1], linewidth=5, label=bold("Thread count"), marker='o', markersize=8)

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

                    savefig(fig, f'./plots/fig/{target}/A-fig-{target}-{ssd}-{metric}.pdf')
                    plt.close(fig)

                # engines
                for wl in ['qd', 't']:
                    if not 'rate' in ssd and 'sync' in vs[ssd] and len(appendix) < 2:
                        for sze in ['4k', '16k', '64k', '1m']:
                            # we call it ducttape, I call it a mistake
                            if 'ssd_d' == ssd and 't' in wl:
                                continue 
                            for en in ['sync', 'posixaio', 'libaio', 'io_uring', 'Polling', 'KernelPolling', 'SPDK']: 
                                if 'qd' in wl:
                                    print(ssd, en, sze, metric, appendix)
                                    print(vs[ssd][en][sze])
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
                            
                            if 't' in wl and 'ssd_b' in ssd:
                                continue

                            fig, ax = plt.subplots()
                            colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                            #print(vs[ssd]['sync']['4k'].keys()); exit(0)
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
                            #plt.legend(ncol=2)

                            ax.set_xlabel(bold("Bandwidth (GiB/s)"))

                            savefig(fig, f'./plots/fig/{target}/engine/fig-{wl}-{sze}-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf')

                            fig, ax = plt.subplots()
                            colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                            #print(vs[ssd]['sync']['4k'].keys()); exit(0)
                            for i, en in enumerate(['libaio', 'io_uring', 'Polling', 'KernelPolling', 'SPDK']):
                                labelen = en
                                if en == "io_uring":
                                    labelen='io_uring + INT'
                                if en == 'Polling':
                                    labelen='io_uring + spoll' 
                                elif en == 'KernelPolling':
                                    labelen='io_uring + cpoll'
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

                            # plt.xlim(0, 5)
                            plt.grid()
                            if sze == '4k' and (not 'p-' in metric):
                                plt.legend(loc=(0.01, 0.42))

                            ax.set_xlabel(bold("Queue depth"))
                            ax.set_xticks(range(1, len([1,2,4,8,16,32,64,128,256])+1), [bold(qdi) for qdi in ['1', '2', '4', '8', '16', '32', '64', '128', '256']])

                            savefig(fig, f'./plots/fig/{target}/engine/fig-{wl}-{sze}-bw{appendix}-SCALING-{target}-{ssd}-{metric}.pdf')


                # dvfs engine
                """
                # if 'enhz' in vs[ssd] and len(appendix) < 2:
                    for bw in [False, True]:
                        for qd in [1, 256]:
                            print(qd)
                            for sz in ['4k', '1m']:
                                print(sz)
                                fig, ax = plt.subplots()
                                colors = [ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                                ci = 0
                                for ei, engine in [(0, 'libaio'), (-1, 'io_uring + INT'), (1, 'poll'), (2, 'kernelpoll'), (-1, 'SPDK')]:
                                    x = []
                                    y = []
                                    z = []
                                    print(engine)
                                    # TO REMOVE
                                    if 'SPDK' in engine and 'ssd_d' in ssd:
                                        continue
                                    for di, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz', 'default']):
                                        x.append(di+0.5)
                                        print(dvfs)
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
                                                # y.append(vs[ssd]['enhz'][dvfs][f'iodepth-{metric}{appendix}'][2*realei + jmp])
                                                jmp = 6 if 'm' in sz else 0
                                                if qd > 1:
                                                    jmp = jmp + 1
                                                # z.append(vs[ssd]['enhz'][dvfs][f'iodepth-b{appendix}'][2*ei + jmp])
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

                                #plt.xlim(0, 7)
                                plt.grid()

                                filename=f'DVFS-{"bw-" if bw else ""}ENG-{sz}-{qd}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf'
                                if sz == '4k' and qd == 256 and (not bw):
                                    filename=f'finaldvfs4k-{ssd}'
                                if sz == '1m' and qd == 256 and (not bw):   
                                    plt.legend()
                                    filename=f'finaldvfs1m-{ssd}'

                                ax.set_xticks([x + 0.5 for x in range(0, 7)], [bold(fq) for fq in ['1', '1.44', '1.88', '2.32', '2.76', '3.2', 'Default']]) 
                                ax.set_xlabel(bold("CPU Frequency (GHz)"))
                                plt.xticks(rotation=45)

                                savefig(fig, f'./plots/fig/{target}/{filename}.pdf')
                """
               
            # DVFS
                if not 'rate' in ssd and '1ghz' in vs[ssd] and len(appendix) < 2:
                    for l1,l2,slab in [(0, 8, '4k'), (9, 17, '1m')]:
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        # print(vs[ssd]['1ghz']); exit(0)
                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(vs[ssd][dvfs][f'iodepth-b{appendix}'][l1:l2], vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][l1:l2], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
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

                        savefig(fig, f'./plots/fig/{target}/DVFS-{slab}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf')
                    
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        # print(vs[ssd]['1ghz']); exit(0)
                        for i, dvfs in enumerate(['1ghz', '1.44ghz', '1.88ghz', '2.32ghz', '2.76ghz', '3.2ghz']):
                            plt.plot(range(1, len(vs[ssd][dvfs][f'iodepth-b{appendix}'][l1:l2+1])+1), vs[ssd][dvfs][f'iodepth-{metric}{appendix}'][l1:l2+1], color=colors[i], linewidth=5, label=bold(dvfs), marker='o', markersize=8)
                        # The missing link
                        if '4k' in slab:
                            plt.plot(range(1, len(vs[ssd][f'qd-b{appendix}'])+1), vs[ssd][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('Default'), marker='o', markersize=8)
                        elif '1m' in slab and 'ssd_d' in ssd:
                            plt.plot(range(1, len(vs[ssd]['io_uring']['1m'][f'qd-b{appendix}'])+1), vs[ssd]['io_uring']['1m'][f'qd-{metric}{appendix}'], color=colors[-1], linewidth=5, label=bold('Default'), marker='o', markersize=8)
                                
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

                        savefig(fig, f'./plots/fig/{target}/DVFS-QD-{slab}-fig-bw{appendix}-{target}-{ssd}-{metric}.pdf')

                    for l1,l2,slab in [(0, 8, '4k'), (9, 17, '1m')]:
                        if 'ssd_d' in ssd:
                            break
                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        # print(vs[ssd]['1ghz']); exit(0)
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

                        savefig(fig, f'./plots/fig/{target}/DVFST-{slab}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf')
                    

                        fig, ax = plt.subplots()
                        colors = ['black', ROSE, CYAN, SAND, TEAL, MAGENTA, 'gray']

                        # print(vs[ssd]['1ghz']); exit(0)
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

                        savefig(fig, f'./plots/fig/{target}/DVFST-T-{slab}-fig-bw{appendix}-correlation-{target}-{ssd}-{metric}.pdf')
                    




print(max_rqs)
