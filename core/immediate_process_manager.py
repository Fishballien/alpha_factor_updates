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
import numpy as np
import traceback


from utils.decorator_utils import run_by_thread
from utils.data_parser import convert_to_lowercase
from utils.calc import if_ticktimes


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
        
        
# %% Processor
class Processor:
    
    def __init__(self, pb_msg):
        header = pb_msg.header
        self.symbol = convert_to_lowercase(header.symbol)
        print(header.symbol, self.symbol)
        self.ts = header.timestamp
        
    
# %% level
class LevelProcessor(Processor):
    
    def __init__(self, pb_msg):
        super().__init__(pb_msg)
        self.dataset = {}
        bid_info, ask_info = pb_msg.bid, pb_msg.ask
        self._get_info_from_level(bid_info, 'bid')
        self._get_info_from_level(ask_info, 'ask')
        
        self.best_found = 0
        self.mpc_found = 0
    
    def _get_info_from_level(self, side_info, side_name):
        side_price_arr = np.array(side_info.price).astype(np.float64)
        try:
            assert len(side_price_arr) > 0
        except AssertionError:
            self.log.error('Empty {side_name} price arr for {symbol}!')
            traceback.print_exc()
        side_volume_arr = np.array(side_info.volume).astype(np.float64)
        side_level_arr = np.array(side_info.level).astype(np.int64)
        try:
            assert (all(np.diff(side_level_arr) > 0) if side_name == 'bid'
                    else all(np.diff(side_level_arr) < 0))
        except AssertionError:
            traceback.print_exc()
        self.dataset[f'{side_name}_price'] = side_price_arr
        self.dataset[f'{side_name}_volume'] = side_volume_arr
        self.dataset[f'{side_name}_level'] = side_level_arr
    
    def get_best_price(self):
        self.dataset['bid1'] = self.dataset['bid_price'][0]
        self.dataset['ask1'] = self.dataset['ask_price'][-1]
        self.best_found = 1
        return self.dataset['bid1'], self.dataset['ask1']
        
    def get_mid_price(self):
        if not self.best_found:
            self.get_best_price()
        self.dataset['mpc'] = (self.dataset['bid1'] + self.dataset['ask1']) / 2
        self.mpc_found = 1
        return self.dataset['mpc']
    
    def get_price_pct(self):
        if not self.mpc_found:
            self.get_mid_price()
        mpc = self.dataset['mpc']
        bid_price_arr = self.dataset['bid_price']
        ask_price_arr = self.dataset['ask_price']
        self.dataset['bid_price_pct'] = (mpc - bid_price_arr) / mpc
        self.dataset['ask_price_pct'] = (ask_price_arr - mpc) / mpc
        return self.dataset['bid_price_pct'], self.dataset['ask_price_pct']
    
    def get_if_ticktimes(self, tick_size, multiplier):
        bid_price_arr = self.dataset['bid_price']
        ask_price_arr = self.dataset['ask_price']
        self.dataset['bid_if_ticktimes'] = if_ticktimes(bid_price_arr, tick_size, multiplier)
        self.dataset['ask_if_ticktimes'] = if_ticktimes(ask_price_arr, tick_size, multiplier)
        return self.dataset['bid_if_ticktimes'], self.dataset['ask_if_ticktimes']
    
    def __getattr__(self, name):
        return self.dataset[name]
        