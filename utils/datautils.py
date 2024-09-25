# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 10:43:21 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import pandas as pd
import numpy as np


# %% add row
def add_row_to_dataframe_reindex(df, new_data, index):
    """
    使用 reindex 将新数据添加到 DataFrame 中，支持动态扩展列，原先没有值的地方填充 NaN。

    参数:
    df (pd.DataFrame): 目标 DataFrame。
    new_data (dict 或 pd.Series): 要添加的新数据，键为列名，值为列值。
    index (str): 新行的索引值。

    无返回值，直接修改 df。
    """
    # 如果 new_data 是字典，转换为 Series
    if isinstance(new_data, dict):
        new_data = pd.Series(new_data)
        
    # 动态扩展列，将结果赋值回 df，并确保未填充的空值为 NaN
    df = df.reindex(columns=df.columns.union(new_data.index, sort=False), fill_value=np.nan)

    # 使用 loc 直接添加数据
    df.loc[index, new_data.index] = new_data
    
    df = df.sort_index()

    return df


# %% align
def align_both(df_1, df_2):
    return align_index(*align_columns(df_1, df_2))


def align_columns(df_1, df_2):
    inner_columns = df_1.columns.intersection(df_2.columns)
    return df_1.loc[:, inner_columns], df_2.loc[:, inner_columns]


def align_index(df_1, df_2):
    inner_index = df_1.index.intersection(df_2.index)
    return df_1.loc[inner_index, :], df_2.loc[inner_index, :]


# %% dict
def is_empty_dict(d):
    # 如果当前字典为空
    if not d:
        return True
    # 遍历字典中的所有值，检查是否为空字典或非空值
    for value in d.values():
        if isinstance(value, dict):
            # 如果值是字典，递归检查该字典是否为空
            if not is_empty_dict(value):
                return False
        else:
            # 如果值不是字典，说明字典不为空
            return False
    return True