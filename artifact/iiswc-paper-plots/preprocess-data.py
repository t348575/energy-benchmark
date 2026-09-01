import json
import numpy as np
import pandas as pd

import os

from common.plot_utils import *

qd_ssd_c="qd-2025-11-06_14-48-52"
qd_ssd_d="qd-2025-11-13_03-53-55"
qd_ssd_e="qd-2025-11-16_20-06-30"
qd_ssd_a="qd-2025-12-02_02-42-57"

rq_ssd_c="request-size-2025-11-06_16-17-20"
rq_ssd_d="request-size-2025-11-12_09-32-33"
rq_ssd_e="request-size-2025-11-16_20-36-10"
rq_ssd_a="request-size-2025-12-02_03-26-10"

a_ssd_c="access-patterns-2025-11-01_19-25-55"
a_ssd_d="access-patterns-2025-11-09_19-42-23"
a_ssd_e="access-patterns-2025-11-18_00-31-53"
a_ssd_a="access-patterns-2025-11-18_00-31-53"

qd_rate_ssd_c="qd-rate-limited-2025-11-06_15-57-05"
qd_rate_ssd_d="qd-rate-limited-2025-11-10_16-03-45"
qd_rate_ssd_e="qd-req-size-rate-limited-2025-11-24_12-57-43"

pst_ssd_c="nvme-pst-eff-2025-11-16_18-44-20"; ssd_c_psts=2
#pst_ssd_d="nvme-pst-eff-2025-11-18_05-43-14"
pst_ssd_e="nvme-pst-eff-2025-11-18_12-32-49"; ssd_e_psts=2
pst_ssd_a="nvme-pst-eff-2025-12-02_00-57-09"; ssd_a_psts=3

psts_ran = {
    'ssd_c' : ssd_c_psts,
    'ssd_e' : ssd_e_psts,
    'ssd_a': ssd_a_psts
}

def parse_bar(filename, ps=0):
    with open(filename) as f:
        j = json.load(f)
        return j['data'][ps]

def parse_bars(filenames, innerind=-1):
    out = []
    for filename in filenames:
        with open(filename) as f:
            j = json.load(f)
            for o in j['data'][0]:
                out.append(o)
    return out


def find_inner_index_bar(filename, label_to_grep):
    with open(filename) as f:
        j = json.load(f)

        comeon = zip(j['data'][0], j['labels'])
        i = 0
        for number, lab in comeon:
            if label_to_grep in lab:
                return i
            i = i + 1
    return -1

def parse_bars_inner(filenames, label_to_grep):
    out = []
    for filename in filenames:
        with open(filename) as f:
            j = json.load(f)

            comeon = zip(j['data'][0], j['labels'])
            for number, lab in comeon:
                if label_to_grep in lab:
                    out.append(number)
                    break 
    return out

def parse_efficiency(filenames, ps=0):
    out = []
    for filename in filenames:
        with open(filename) as f:
            j = json.load(f)
            for x in j:
                out.append(x[ps])
    return out

def parse_ind_efficiency(filenames, ind, ps=0):
    out = []
    for filename in filenames:
        with open(filename) as f:
            j = json.load(f)
            i = 0
            for x in j:
                if i == ind:
                    out.append(x[ps])
                i = i + 1
    return out

def parse_bw(filename):
    o = 0
    with open(filename) as f:
        j = json.load(f)
        o = sum([job['read']['bw_mean'] / (2**20) for job in j['jobs']]) + sum([job['write']['bw_mean'] / (2**20) for job in j['jobs']])
    return o

max_rqs = {
 'ssd_c': 0,
 'ssd_d': 0,
 'ssd_e': 0,
 'ssd_e_rate': 0,   
 'ssd_a': 0,   
 'ssd_a_rate': 0   
}
def verify_max_rq(filename, ssd):
    o = 0
    with open(filename) as f:
        j = json.load(f)
        ut = j['disk_util'][0]
        rq_size =  ut['read_sectors'] / ut['read_ios']
        print(ssd, rq_size/2, filename)
        if rq_size > max_rqs[ssd]:
            max_rqs[ssd] = rq_size
    return o

def parse_lat(filename):
    with open(filename) as f:
        j = json.load(f)
        return j['jobs'][0]['read']['clat_ns']['percentile']['95.000000']

def parse_bw_dev(filename):
    with open(filename) as f:
        j = json.load(f)
        return (sum([job['read']['bw_dev']**2 for job in j['jobs']]) / len(j['jobs'])) ** 0.5

# Workaround for inconsistent benchmark runs
def parametrize_result_dir(prefix, suffix, y=False, ran=None):
    if y:
        return [f'{prefix}-{i}-{suffix}' for i in ran]
    else:
        return [f'{prefix}-{suffix}']

# Workaround for ordering issues
def reswaparray(a):
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

ws = {}
for target, suffix in [('ssd', 'bytes'), ('cpu', 'only-cpu-bytes'), ('both', '+cpu-bytes'), ('system', None)]:
    #
    vs={}
    vs['ssd_c']={}; vs['ssd_c_rate']={}
    vs['ssd_d']={}; vs['ssd_d_rate']={}
    vs['ssd_e']={}; vs['ssd_e_rate']={}
    vs['ssd_a']={}; vs['ssd_a_rate']={}

    # QD
    for access, qappendix in [('', ''), ('seqread-', '-seq')]:
        for label, qd in [('ssd_a', qd_ssd_a), ('ssd_c', qd_ssd_c), ('ssd_d', qd_ssd_d), ('ssd_e', qd_ssd_e), ('ssd_c_rate', qd_rate_ssd_c), ('ssd_d_rate', qd_rate_ssd_d), ('ssd_e_rate', qd_rate_ssd_e)]:
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
        for label, qd in [('ssd_a', qd_ssd_a), ('ssd_c', qd_ssd_c), ('ssd_d', qd_ssd_d), ('ssd_e', qd_ssd_e), ('ssd_c_rate', qd_rate_ssd_c), ('ssd_d_rate', qd_rate_ssd_d), ('ssd_e_rate', qd_rate_ssd_e)]:
            parame = 'rate' in label
            if policy != "randread-threads" and label != 'ssd_c':
                continue

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
        for label, qd in [('ssd_a', rq_ssd_a), ('ssd_c', rq_ssd_c), ('ssd_d', rq_ssd_d), ('ssd_e', rq_ssd_e), ('ssd_e_rate', qd_rate_ssd_e)]:
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
                ('ssd_a', \
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
        for label, qds in [\
                ('ssd_a', \
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
            if 'enhz' not in vs[label]:
                vs[label]['enhz'] = {}
            for qd in qds:
                hz=qd.split('-')[1]
                vs[label]['enhz'][hz] = {}
                for access in ['iodepth']:
                    if not 'system' in target:
                        vs[label]['enhz'][hz][f'{access}-e'] = parse_efficiency([f"results/{qd}/plots/efficiency/plot_data/{access}-ioengines-{suffix}-j.json"])
                    vs[label]['enhz'][hz][f'{access}-b'] = [parse_bw(f"results/{qd}/data/{access}-ioengines-ps0-i0-{i}/results.json") for i in range(0, 12)]
                    if 'ssd' in target:
                        # power SSD
                        vs[label]['enhz'][hz][f'{access}-p-ssd'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-ssd.bar.json")
                    elif 'cpu' in target:
                        # power CPU
                        vs[label]['enhz'][hz][f'{access}-p-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-cpu.bar.json")
                        # load cpu
                        vs[label]['enhz'][hz][f'{access}-l-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-load.bar.json")
                        # stdev
                        vs[label]['enhz'][hz][f'{access}-pdev-cpu'] = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-stdev-cpu.bar.json")
                    elif 'both' in target:
                        # power SSD
                        ssd = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-ssd.bar.json")
                        # power CPU
                        cpu = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-cpu.bar.json")
                        vs[label]['enhz'][hz][f'{access}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
                    elif 'system' in target:
                        system = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-ioengines-system.bar.json")
                        vs[label]['enhz'][hz][f'{access}-p-system'] = system
                        vs[label]['enhz'][hz][f'{access}-e'] = [ (x*1024)/y for x,y in zip(vs[label][hz][f'{access}-b'], system)]

        for label, qds in [\
                ('ssd_a', \
                    ['dvfs-1ghz-spdk-2025-12-02_15-22-31', \
                     'dvfs-1.44ghz-spdk-2025-12-02_15-18-57', \
                     'dvfs-1.88ghz-spdk-2025-12-02_15-15-26', \
                     'dvfs-2.32ghz-spdk-2025-12-02_15-11-57', \
                     'dvfs-2.76ghz-spdk-2025-12-02_15-08-31', \
                     'dvfs-3.2ghz-spdk-2025-12-02_14-59-40']),\
                ('ssd_d', \
                    ['dvfs-1ghz-spdk-2025-12-07_00-17-28', \
                     'dvfs-1.44ghz-spdk-2025-12-05_22-59-00', \
                     'dvfs-1.88ghz-spdk-2025-12-05_22-55-27', \
                     'dvfs-2.32ghz-spdk-2025-12-05_22-51-57', \
                     'dvfs-2.76ghz-spdk-2025-12-05_22-48-29', \
                     'dvfs-3.2ghz-spdk-2025-12-05_22-45-01']),\
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
                        vs[label]['enhz'][hz][f'{access}-p-both'] = [x + y for x, y in zip(ssd, cpu)]
                    elif 'system' in target:
                        system = parse_bar(f"results/{qd}/plots/power/plot_data/{access}-system.bar.json")
                        vs[label]['spdkhz'][hz][f'{access}-p-system'] = system
                        vs[label]['spdkhz'][hz][f'{access}-e'] = [ (x*1024)/y for x,y in zip(vs[label][hz][f'{access}-b'], system)]
    
        # engine
        for label, qd, wl in [\
            ('ssd_e', 'io-engine-threads-2025-11-16_16-49-23', 't'),\
            ('ssd_e', 'io-engine-iodepth-2025-11-16_13-59-14', 'qd'),\
            ('ssd_d', 'io-engine-iodepth-2025-11-12_18-25-25', 'qd'),\
            ('ssd_d', 'io-engine-threads-2025-11-12_07-21-54', 't'),\
            ('ssd_a', 'io-engine-iodepth-2025-12-02_04-23-52', 'qd'),\
            ('ssd_a', 'io-engine-threads-2025-12-02_06-16-26', 't')\
            ]:
            for ei, engine in enumerate(['sync', 'posixaio', 'libaio', 'io_uring']):
                if engine not in vs[label]:
                    vs[label][engine] = {}
                for si, size in enumerate(['4k', '16k', '64k', '1m']):
                    if size not in vs[label][engine]:
                        vs[label][engine][size] = {}

                    jj = si * 4
                    index = jj + ei

                    greplabel = f"{engine} {size}"
                    if 'ssd_d' in label:
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
                    if 'system' in target:
                        vs[label][engine][size][f'{wl}-e'] = [ (x*1024)/y for x,y in zip(b, p)]
                    else:
                        ind = find_inner_index_bar(f"results/{qd}/plots/power/plot_data/read-1-ssd.bar.json", label_to_grep=greplabel)
                        out = parse_ind_efficiency([f"results/{qd}/plots/efficiency/plot_data/read-{2**i}-{suffix}-j.json" for i in range(0,9)], ind)
                        vs[label][engine][size][f'{wl}-e'] = out

        for label, qd, wl in [\
            ('ssd_e', 'io-engine-iouring-iodepth-2025-11-16_15-52-10', 'qd'), \
            ('ssd_d', 'io-engine-iouring-iodepth-2025-11-12_20-30-44', 'qd'), \
            ('ssd_d', 'io-engine-iouring-threads-2025-11-12_08-49-13', 't'),\
            ('ssd_a', 'io-engine-iouring-iodepth-2025-12-02_00-00-45', 'qd') \
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

                    greplabel = f"{engine} {size}"
                    if label == 'ssd_d':
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
                    if 'system' in target:
                        vs[label][engine][size][f'{wl}-e'] = [ (x*1024)/y for x,y in zip(b, p)]
                    else:
                        ind = find_inner_index_bar(f"results/{qd}/plots/power/plot_data/read-1-ssd.bar.json", label_to_grep=greplabel)
                        out = parse_ind_efficiency([f"results/{qd}/plots/efficiency/plot_data/read-{2**i}-{suffix}-j.json" for i in range(0,9)], ind)
                        vs[label][engine][size][f'{wl}-e'] = out

        for label, qd, wl in [\
                ('ssd_d', 'spdk-2025-11-13_09-32-00', 'qd'),\
                ('ssd_e', 'spdk-2025-11-16_21-00-56', 'qd'),\
                ('ssd_a', 'spdk-2025-12-02_03-50-30', 'qd')\
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
                    greplabel = f"{size}"
                    if label == 'ssd_d':
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
                    if 'system' in target:
                        vs[label][engine][size][f'{wl}-e'] = [ (x*1024)/y for x,y in zip(b, p)]
                    else:
                        ind = find_inner_index_bar(f"results/{qd}/plots/power/plot_data/read-1-ssd.bar.json", label_to_grep=greplabel)
                        out = parse_ind_efficiency([f"results/{qd}/plots/efficiency/plot_data/read-{2**i}-{suffix}-j.json" for i in range(0,9)], ind)
                        vs[label][engine][size][f'{wl}-e'] = out
        # PST
        for label, qd, psts in [('ssd_c', pst_ssd_c, ssd_c_psts), ('ssd_e', pst_ssd_e, ssd_e_psts), ('ssd_a', pst_ssd_a, ssd_a_psts)]:
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
        ws[target] = vs

with open(f"iiswcdata/preprocessed-ebench-data.json", "w") as f:
    json.dump(ws, f, indent=4)