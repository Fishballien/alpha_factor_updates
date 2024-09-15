# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 10:28:14 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import re
from google.protobuf.message import DecodeError


# %%
__all__ = ['ZMQ_TOPIC_LEN', 'parse_header', 'deserialize_pb']


# %%
ZMQ_TOPIC_LEN = 32  # Topic长度，固定为32字节


# %%
def parse_header(msg): # fr xwy
    try:
        topic = msg[:ZMQ_TOPIC_LEN].decode('utf-8', errors='ignore')  # 解析topic
        return topic
    except UnicodeDecodeError as e:
        print(f"Failed to decode topic: {e}")
        return None


def deserialize_pb(data, pb_class): # fr xwy
    pb_msg = pb_class()
    try:
        pb_msg.ParseFromString(data)
        return pb_msg
    except DecodeError as e:
        print(f"Failed to parse protobuf message: {e}")
        return None
    
    
def convert_to_lowercase(symbol):
    # 使用正则表达式匹配 .BN 前面的部分，允许字母和数字的组合
    match = re.search(r'(\w+)\.BN$', symbol)
    
    if match:
        # 提取匹配到的内容，并将其转化为小写
        return match.group(1).lower()
    
    return symbol  # 如果没有匹配到 .BN，返回原值
