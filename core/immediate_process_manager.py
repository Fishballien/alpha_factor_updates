# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 16:21:26 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
from abc import ABC, abstractmethod


from utils.decorator_utils import run_by_thread


# %%
class ImmediateProcessManager(ABC):
    
    def __init__(self, topic_list, msg_controller, log=None):
        self.topic_list = topic_list
        self.topic_func_mapping = {}
        self.msg_controller = msg_controller
        self.log = log
        
        self._init_container()
        self._init_topic_func_mapping()
        self._running = True
        
    @abstractmethod
    def _init_container(self):
        pass
    
    @abstractmethod
    def _init_topic_func_mapping(self):
        pass
    
    @run_by_thread
    def _loop_processing(self, topic):
        while self._running:
            pb_msg = self.msg_controller[topic].get()
            self.topic_func_mapping[topic](pb_msg)
            
    def start(self):
        for topic in self.topic_list:
            self._loop_processing(topic)
            
    def stop(self):
        self._running = False