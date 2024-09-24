# -*- coding: utf-8 -*-
"""
Created on Fri Sep 13 15:54:44 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import numpy as np
import pandas as pd
from numba import njit


# %% 2 factors
def calc_imb(a, b):
    if a + b == 0:
        return np.nan
    return (a - b) / (a + b)


def calc_side_ratio(s, a, b):
    if a + b == 0:
        return np.nan
    return s / (a + b)


# %% tick related
# @njit()
def is_integer_price(price, tick_size, multiplier):
    """
    判断给定的价格是否为“整数”价格。
    
    参数：
    price (float) - 需要判断的价格
    tick_size (float) - 最小变动单位
    
    返回值：
    bool - 如果价格是“整数”价格，返回True，否则返回False
    """
    # 计算整数价格的单位
    integer_price_unit = multiplier * tick_size
    # 判断价格是否为整数价格
    return abs(price % integer_price_unit) < tick_size / 2


def if_ticktimes(price_arr, tick_size, multiplier):
    if_ticktimes_arr = np.zeros(len(price_arr), dtype=np.int32)
    for i_p, px in enumerate(price_arr):
        if is_integer_price(px, tick_size, multiplier):
            if_ticktimes_arr[i_p] = 1
    return if_ticktimes_arr


# %% slope
@njit("float64(float64[:], float64[:])")
def compute_slope(x, y):
    n = len(x)
    if n != len(y):
        raise ValueError("The length of x and y must be the same")

    sum_x = np.sum(x)
    sum_y = np.sum(y)
    sum_xy = np.sum(x * y)
    sum_xx = np.sum(x * x)
    
    numerator = n * sum_xy - sum_x * sum_y
    denominator = n * sum_xx - sum_x * sum_x
    
    if denominator == 0:
        return np.nan

    slope = numerator / denominator
    return slope


# %% min ratio
def calculate_1min_ratio(df, interval_minutes):
    """
    计算每个整 interval_minutes 时间戳的前 interval_minutes 分钟的第一分钟成交量占前 interval_minutes 分钟总成交量的占比。
    
    参数:
    df (pd.DataFrame): 包含时间戳索引和不同币种成交量的 DataFrame。
    interval_minutes (int): 整数分钟数，必须能被5整除且大于0。
    
    返回:
    pd.DataFrame: 包含每个整 interval_minutes 时间戳的前 interval_minutes 分钟的第一分钟成交量占前 interval_minutes 分钟总成交量的占比。
    """
    if interval_minutes <= 0 or interval_minutes % 5 != 0:
        raise ValueError("interval_minutes 必须大于0且能被5整除")
    
    # 找到最近的整半小时时间戳
    if len(df) == 0:
        return pd.DataFrame(columns=df.columns)
    start_time = df.index[0]
    if start_time.minute % 30 != 0:
        # 如果不是整半小时，找到最早的整半小时
        next_half_hour = (start_time + pd.Timedelta(minutes=(30 - start_time.minute % 30))).replace(second=0)
        df = df[df.index >= next_half_hour]
        if len(df) == 0:
            return pd.DataFrame(columns=df.columns)
    
    # 计算每个 interval_minutes 时间区间的总成交量
    total_volume = df.rolling(window=interval_minutes, min_periods=interval_minutes).sum()
    
    # 计算每个 interval_minutes 时间区间的第一分钟成交量
    first_minute_volume = df.shift(interval_minutes - 1)
    
    # 计算占比
    ratio = first_minute_volume / total_volume
    
    # 只保留每个 interval_minutes 的时间戳
    ratio = ratio[df.index.minute % interval_minutes == 0]
    
    return ratio


# %%
def ts_avg(df: pd.DataFrame, ts, mmt_wd):
    return df.loc[ts-mmt_wd:].mean(axis=0)


def ts_int(df: pd.DataFrame, ts, mmt_wd):
    return df.loc[ts-mmt_wd:].mean(axis=0) / df.loc[ts-mmt_wd:].std(axis=0).replace(0, np.nan)


def ts_std2avg(df: pd.DataFrame, ts, mmt_wd):
    return df.loc[ts-mmt_wd:].std(axis=0) / df.loc[ts-mmt_wd:].mean(axis=0).replace(0, np.nan)


def ts_skew(df: pd.DataFrame, ts, mmt_wd):
    return df.loc[ts-mmt_wd:].skew(axis=0)


def ts_kurt(df: pd.DataFrame, ts, mmt_wd):
    return df.loc[ts-mmt_wd:].kurt(axis=0)


def ts_P25OverP75(df: pd.DataFrame, ts, mmt_wd):
    return df.loc[ts-mmt_wd:].quantile(0.25) / df.loc[ts-mmt_wd:].quantile(0.75).replace(0, np.nan)


_ts_stats_dict = {
    'avg': ts_avg,
    'int': ts_int,
    'std2avg': ts_std2avg,
    'skew': ts_skew,
    'kurt': ts_kurt,
    'P25OverP75': ts_P25OverP75,
    }


def ts_basic_stat(df: pd.DataFrame, ts, mmt_wd, stats_type='avg'):
    return _ts_stats_dict[stats_type](df, ts, mmt_wd)
