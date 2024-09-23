# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 10:34:59 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports


# %%
def _check_if_valid_topic(topic_name, address_topics):
    if not any(topic_name.startswith(prefix) for prefix in address_topics):
        return 0
    return 1

    
def handler_msg_fr_lord(socket, log, address_topics):
    # 使用 recv_multipart 接收多帧消息
    frames = socket.recv_multipart()
    if len(frames) != 2:
        log.error(f"Expected 2 frames but received {len(frames)}")
        return 0
    
    # 第一帧是 topic，第二帧是 body 数据
    topic_name = frames[0].decode("utf-8")  # 假设 topic 是 UTF-8 编码
    data = frames[1]  # 第二帧是实际的数据
    if not topic_name:
        return 0
    if not _check_if_valid_topic(topic_name, address_topics):
        log.error(f"Unknown topic: {topic_name}")
        return 0
    
    return topic_name, data
            

def handler_msg_fr_cluster(socket, log, address_topics):
    topic_name = socket.recv_string()
    data = socket.recv()
    
    if not _check_if_valid_topic(topic_name, address_topics):
        log.error(f"Unknown topic: {topic_name}")
        return 0
    
    return topic_name, data