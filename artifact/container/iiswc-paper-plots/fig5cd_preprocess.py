import numpy as np
import json

for out, dirp in [\
        ('randw-ssd_a', 'iiswcdata/randw-ssda'),\
        ('randw-ssd_d', 'iiswcdata/randw-ssdd'),\
        ('rw-ssd_d', 'iiswcdata/seqw-ssdd'),\
        ('rw-ssd_a', 'iiswcdata/rw-ssda'),\
        ('seqw-ssd_d', 'iiswcdata/seqw-ssdd'),\
        ('seqw-ssd_a', 'iiswcdata/seqw-ssda')\
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
