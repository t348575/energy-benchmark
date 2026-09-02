import numpy as np
import json
import argparse
import os

parser = argparse.ArgumentParser()
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
parser.add_argument('--in-dir', nargs='?', const=f"{BASE_DIR}/results",
    default=f"{BASE_DIR}/results", type=str)
args = parser.parse_args()
in_dir = args.in_dir

for out, dirp in [\
        ('randw-ssd_a', f'{in_dir}/access-patterns-randwrite-2025-12-04_21-53-48/data/rand-write-ps0-i0-0'),\
        ('randw-ssd_d', f'{in_dir}/access-patterns-randwrite-2025-12-06_00-00-05/data/rand-write-limited-ps0-i0-0/'),\
        ('rw-ssd_d', f'{in_dir}/access-patterns-readwrite-2025-12-06_01-29-26/data/read-write-limited-ps0-i0-0'),\
        ('rw-ssd_a', f'{in_dir}/access-patterns-readwrite-2025-12-05_05-29-01/data/read-write-ps0-i0-0/'),\
        ('seqw-ssd_d', f'{in_dir}/access-patterns-seqwrite-2025-12-07_00-21-10/data/seq-write-ps0-i0-0'),\
        ('seqw-ssd_a', f'{in_dir}/access-patterns-seqwrite-2025-12-05_01-40-57/data/seq-write-ps0-i0-0')\
        ]:
    bw_x = []
    bw_y = []
    with open(f"{dirp}/log_bw.1.log", 'r') as file:
        data = [line.rstrip() for line in file]
        bw_x = [0.] * len(data)
        bw_y = [0.] * len(data)
        for idx, line in enumerate(data):
            spl = line.split()
            bw_x[idx] = (float(spl[0][:-1])) / 1000.
            bw_y[idx] = (float(spl[1][:-1])) / (1024. * 1024.)
        bw_x = np.array(bw_x)
        bw_y = np.array(bw_y)

    p_x = []
    p_y = []
    with open(f'{dirp}/powersensor3.csv', 'r') as file:
        data = [line.rstrip() for line in file]
        p_x = [0.] * len(data)
        p_y = [0.] * len(data)
        for idx, line in enumerate(data):
            if not idx:
                continue
            spl = line.split(',')
            p_x[idx] = (float(spl[0])) / 1000.
            p_y[idx] = (float(spl[1]))
        p_x = np.array(p_x)
        p_y = np.array(p_y)
    
    preprocessed = {
        'bw_x': bw_x.tolist(),
        'bw_y': bw_y.tolist(),
        'p_x': p_x.tolist(),
        'p_y': p_y.tolist()
    }

    with open(f'./iiswcdata/preprocessed-fig5cd-{out}.json', 'w') as f:
        json.dump(preprocessed, f, indent=4)
