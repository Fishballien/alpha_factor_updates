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
from numba import njit, prange


from utils.datautils import align_both


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
# =============================================================================
# def is_integer_price(price, tick_size, multiplier):
#     """
#     判断给定的价格是否为“整数”价格。
#     
#     参数：
#     price (float) - 需要判断的价格
#     tick_size (float) - 最小变动单位
#     
#     返回值：
#     bool - 如果价格是“整数”价格，返回True，否则返回False
#     """
#     # 计算整数价格的单位
#     integer_price_unit = multiplier * tick_size
#     # 判断价格是否为整数价格
#     return abs(price % integer_price_unit) < tick_size / 2
# =============================================================================


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
    int_price = round(price / tick_size) 
    # 判断价格是否为整数价格
    return abs(int_price % multiplier) < 1


def if_ticktimes(price_arr, tick_size, multiplier):
    if_ticktimes_arr = np.zeros(len(price_arr), dtype=np.int32)
    for i_p, px in enumerate(price_arr):
        # if i_p == 6:
        #     breakpoint()
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


# %% ts
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


# %% reg
def ts_regress_once(dfy: pd.DataFrame, dfx: pd.DataFrame):
    # 计算列的均值，跳过 NaN 值
    yAvg = dfy.mean(axis=0, skipna=True)
    xAvg = dfx.mean(axis=0, skipna=True)

    # 中心化数据
    xdm = dfx.subtract(xAvg, axis=1)
    ydm = dfy.subtract(yAvg, axis=1)

    # 计算点积，跳过 NaN 值
    ydm_dot_xdm = np.nansum(ydm * xdm, axis=0)
    xdm_dot_xdm = np.nansum(xdm * xdm, axis=0)

    # 计算斜率和截距
    slope = np.divide(ydm_dot_xdm, xdm_dot_xdm, out=np.zeros_like(ydm_dot_xdm), where=xdm_dot_xdm!=0)
    intercept = yAvg - slope * xAvg

    # 计算最后一行的残差
    last_row_y = dfy.iloc[-1].values
    last_row_x = dfx.iloc[-1].values
    resid_last = last_row_y - (slope * last_row_x + intercept)

    return {'slope': pd.Series(slope, index=dfx.columns), 
            'intercept': pd.Series(intercept, index=dfx.columns), 
            'resid': pd.Series(resid_last, index=dfx.columns)}


def safe_ts_regress_once(dfy: pd.DataFrame, dfx: pd.DataFrame):
    dfy, dfx = align_both(dfy, dfx)
    if len(dfy) == 0:
        return None
    return ts_regress_once(dfy, dfx)


@njit("float64[:](float64[:, :], int64)")
def nanmean_2d(arr, axis):
    if axis == 0:
        result = np.zeros(arr.shape[1])
        for j in prange(arr.shape[1]):
            total = 0.0
            count = 0
            for i in range(arr.shape[0]):
                if not np.isnan(arr[i, j]):
                    total += arr[i, j]
                    count += 1
            result[j] = total / count if count > 0 else np.nan
        return result
    elif axis == 1:
        result = np.zeros(arr.shape[0])
        for i in prange(arr.shape[0]):
            total = 0.0
            count = 0
            for j in range(arr.shape[1]):
                if not np.isnan(arr[i, j]):
                    total += arr[i, j]
                    count += 1
            result[i] = total / count if count > 0 else np.nan
        return result
    else:
        raise ValueError("Axis must be 0 or 1")


@njit("float64[:](float64[:, :], int64)")
def nansum_2d(arr, axis):
    if axis == 0:
        result = np.zeros(arr.shape[1])
        for j in prange(arr.shape[1]):
            total = 0.0
            for i in range(arr.shape[0]):
                if not np.isnan(arr[i, j]):
                    total += arr[i, j]
            result[j] = total
        return result
    elif axis == 1:
        result = np.zeros(arr.shape[0])
        for i in prange(arr.shape[0]):
            total = 0.0
            for j in range(arr.shape[1]):
                if not np.isnan(arr[i, j]):
                    total += arr[i, j]
            result[i] = total
        return result
    else:
        raise ValueError("Axis must be 0 or 1")
        
        
@njit("Tuple((float64[:, :], float64[:, :], float64[:, :]))(float64[:, :], float64[:, :], int64, int64)")
def ts_regress_step_forward_nb(dfy_val: np.ndarray, dfx_val: np.ndarray, window=48, step=1):
    slope_res = np.full(dfx_val.shape, np.nan, dtype=np.float64)
    intercept_res = np.full(dfx_val.shape, np.nan, dtype=np.float64)
    resid_res = np.full(dfx_val.shape, np.nan, dtype=np.float64) # 保存数据的数组
    
    ## calculate residues row by row
    for rk in prange(window, len(dfy_val), step):
        yView = dfy_val[rk - window: rk, :]
        xView = dfx_val[rk - window: rk, :]

        yAvg = nanmean_2d(yView, 0)
        xAvg = nanmean_2d(xView, 0)

        xdm = xView - xAvg
        ydm = yView - yAvg

        ydm_dot_xdm = nansum_2d(ydm * xdm, 0)
        xdm_dot_xdm = nansum_2d(xdm * xdm, 0)

        slope = np.divide(ydm_dot_xdm, xdm_dot_xdm)  # slope (beta)
        intercept = yAvg - slope * xAvg  # intercept

        slope_res[rk, :] = slope
        intercept_res[rk, :] = intercept
        resid_res[rk, :] = yView[-1, :] - slope * xView[-1, :] - intercept

    return slope_res, intercept_res, resid_res


def safe_ts_regress_step_forward(dfy: pd.DataFrame, dfx: pd.DataFrame, window=48, step=1):
    dfy, dfx = align_both(dfy, dfx)
    slope_res, intercept_res, resid_res = ts_regress_step_forward_nb(
        dfy.values, dfx.values, window=window, step=step)
    return {
        'slope': pd.DataFrame(slope_res, index=dfy.index, columns=dfy.columns),
        'intercept': pd.DataFrame(intercept_res, index=dfy.index, columns=dfy.columns),
        'resid': pd.DataFrame(resid_res, index=dfy.index, columns=dfy.columns),
        }


# %% cut last
def get_last_valid_values_from_index(df: pd.DataFrame, idx: int) -> pd.Series:
    # 使用loc从idx开始取所有行
    recent_array = df.loc[idx:, :].to_numpy()
    # 从最后一行开始逐列查找第一个非NaN值
    result = np.full(df.shape[1], np.nan)  # 初始化结果为NaN
    for col in range(recent_array.shape[1]):
        valid_mask = ~np.isnan(recent_array[:, col])  # 检查非NaN值
        if valid_mask.any():
            # 找到最后一个非NaN值
            result[col] = recent_array[valid_mask, col][-1]
    
    return pd.Series(result, index=df.columns)