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


# %% 2 factors
def calc_imb(a, b):
    return (a - b) / (a + b)


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