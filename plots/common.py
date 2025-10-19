import os
import re
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from matplotlib import rcParams
rcParams['font.size'] = 12

sns.set_palette("colorblind")
colors = ['#0173b2', '#de8f05', '#029e73', '#d55e00', '#cc78bc', '#ca9161', '#fbafe4', '#949494', '#ece133', '#56b4e9']

def _ffill_limit_nonzero(s: pd.Series, limit: int) -> pd.Series:
    s2 = s.where(s != 0)
    return s2.ffill(limit=limit).fillna(0)

def fill_clean(df, offset=0, trim=0, fillmode="ffill", fillmodespread=1000):
    df = df.drop_duplicates(subset="time", keep=False)
    df.set_index("time", inplace=True)

    max_idx = int(df.index.max())
    full_idx = pd.RangeIndex(0, max_idx + 1, step=1)
    df = df.reindex(full_idx)

    if offset > 0:
        df = df[(df.index >= offset) & (df.index < trim)]
        if df.empty:
            return df
        df.index = df.index - df.index[0]

    df = df.reset_index().rename(columns={"index": "time"})
    df["time"] = df["time"] / 1000

    if fillmode == "ffill":
        df = df.ffill()
    elif fillmode == "0s":
        df = df.fillna(0)
    elif fillmode == "spread":
        num_cols = df.select_dtypes(include=[np.number]).columns.drop("time", errors="ignore")
        for col in num_cols:
            df[col] = _ffill_limit_nonzero(df[col], limit=fillmodespread)
        df[num_cols] = df[num_cols].fillna(0)
        # for col in num_cols:
        #     buffer = []
        #     current_val = None
        #     remaining = 0
        #     for v in df[col]:
        #         if pd.notna(v) and v != 0:
        #             current_val = v
        #             remaining = fillmodespread
        #             buffer.append(v)
        #         else:
        #             if remaining > 0 and current_val is not None:
        #                 buffer.append(current_val)
        #                 remaining -= 1
        #             else:
        #                 buffer.append(np.nan)
        #     df[col] = buffer
        # df.fillna({col: 0 for col in num_cols}, inplace=True)
    else:
        raise ValueError(f"Unknown fillmode: {fillmode!r}")

    return df

def offset_trace_time(data, data_path):
    t0 = data["time"].min()
    out = data.copy()
    out.loc[:, "time"] = ((out["time"] - t0) / 1e6).round().astype(int)

    offset_file = os.path.join(data_path, "trace_offset")
    if os.path.exists(offset_file):
        with open(offset_file) as f:
            s = int(f.readline().strip() or 0)
        out.loc[:, "time"] = out["time"] + s
    return out

def parse_time_string(ramp_time_str):
    match = re.match(r"(\d+)([smh])", ramp_time_str)
    if not match:
        return 0
    value, unit = match.groups()
    value = int(value)
    if unit == "s":
        return value * 1000
    elif unit == "m":
        return value * 60 * 1000
    elif unit == "h":
        return value * 60 * 60 * 1000
    else:
        return 0