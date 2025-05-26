import os
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

sns.set_palette("colorblind")
colors = ['#0173b2', '#de8f05', '#029e73', '#d55e00', '#cc78bc', '#ca9161', '#fbafe4', '#949494', '#ece133', '#56b4e9']
# TODO: fetch these
power_states = ['ps0 (8.5W)', 'ps1 (4.5W)', 'ps2 (3.2W)']

agg_options = {
    'count': 'sum',
    'function': 'first',
    'is_nvme_call': 'first',
    'has_fs_pagewrite': 'first',
    'requeued_io': 'first',
    'vfs_read': 'first',
    'vfs_write': 'first',
    'vfs_fsync': 'first'
}

def fill_clean(df, offset=0, trim=0, fillmode="ffill"):
    df = df.drop_duplicates(subset="time", keep=False)
    df.set_index("time", inplace=True)

    full_idx = np.arange(0, df.index.max() + 1, 1)
    df = df.reindex(full_idx)

    if offset > 0:
        df = df[df.index >= offset]
        df = df[df.index < trim]
        if len(df) == 0:
            return df
        df.index = df.index - df.index[0]

    df = df.reset_index().rename(columns={"index": "time"})
    df["time"] = df["time"] / 1000

    if fillmode == "ffill":
        df = df.ffill()
    elif fillmode == "0s":
        df.fillna(0, inplace=True)
    elif fillmode == "spread1000":
        num_cols = df.select_dtypes(include=[np.number]).columns.drop("time")
        for col in num_cols:
            buffer = []
            current_val = None
            remaining = 0
            for v in df[col]:
                if pd.notna(v) and v != 0:
                    current_val = v
                    remaining = 1000
                    buffer.append(v)
                else:
                    if remaining > 0 and current_val is not None:
                        buffer.append(current_val)
                        remaining -= 1
                    else:
                        buffer.append(np.nan)
            df[col] = buffer
        df.fillna({col: 0 for col in num_cols}, inplace=True)
    else:
        raise ValueError(f"Unknown fillmode: {fillmode!r}")

    return df

def offset_trace_time(data, data_path):
    t0 = data["time"].min()
    data.loc[:, "time"] = ((data["time"] - t0) / 1e6).round().astype(int)
    offset_file = os.path.join(data_path, "trace_offset")
    if os.path.exists(offset_file):
        with open(offset_file) as f:
            s = int(f.readline())
        data.loc[:, "time"] += s
    return data