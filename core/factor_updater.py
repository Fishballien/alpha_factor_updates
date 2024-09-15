# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 10:57:01 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import os
from pathlib import Path
import toml
import itertools
import time
import sys
from abc import ABC, abstractmethod
import signal
import threading


from receiver.rcv_fr_lord import LordWithFilter
from core.database_handler import DatabaseHandler
from utils.logutils import FishStyleLogger
from utils.dirutils import load_path_config
from utils.market import load_binance_data, get_binance_tick_size, usd


# %%
class FactorUpdater(ABC):
    
    def __init__(self):
        self._load_path_config()
        self._init_dir()
        self._load_param()
        self._init_log()
        self._init_msg_controller()
        self._init_database_handler()
        self._init_param_set()
        self._set_up_signal_handler()
        self.running = True
        
    def _load_path_config(self):
        file_path = Path(__file__).resolve()
        project_dir = file_path.parents[1]
        self.path_config = load_path_config(project_dir)
        
    def _init_dir(self):
        path_config = self.path_config
        
        self.param_dir = Path(path_config['param'])
        cache_dir = Path(path_config['cache'])
        persist_dir = Path(path_config['persist'])
        self.cache_dir = cache_dir / self.name 
        self.persist_dir = persist_dir / self.name
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_param(self):
        self.params = toml.load(self.param_dir / f'{self.name}.toml')
        
        self.address = self.params['address']
        self.topic_list = self.params['topic']
        self.mysql_name = self.params['mysql_name']
        self.author = self.params['author']
        self.category = self.params['category']
        
    def _init_param_set(self):
        factor_related = self.params.get('factors_related', {})
        if factor_related:
            final_param = factor_related['final']
            if final_param:
                self.param_set = para_allocation(final_param)

    def _init_log(self):
        self.log = FishStyleLogger()
        
    def _init_msg_controller(self):
        self.msg_controller = LordWithFilter(self.topic_list, address=self.address, log=self.log)
        
    def _init_database_handler(self):
        self.db_handler = DatabaseHandler(self.mysql_name, log=self.log)
        
    def _set_up_signal_handler(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGSEGV, self._signal_handler)
        signal.signal(signal.SIGILL, self._signal_handler)
        
    def _signal_handler(self, sig, frame):
        self.log.warning("收到终止信号，正在清理资源...")
        self.stop()
        time.sleep(15) # HINT: 设置一定的等待时间，等待task schedule执行完毕后终止
        sys.exit(0)
        
    @abstractmethod
    def run(self):
        pass
    
    @abstractmethod
    def stop(self):
        pass
    
    
class FactorUpdaterWithTickSize(FactorUpdater):
    
    def __init__(self):
        super().__init__()
        self._init_exchange()
        self.reload_tick_size_mapping()
        
    def _init_exchange(self):
        exchange = self.params['exchange']
        self.exchange_info_dir = Path(self.path_config['exchange_info'])
        self.exchange = globals()[exchange]
        
    def reload_tick_size_mapping(self):
        exchange_info = load_binance_data(self.exchange, self.exchange_info_dir)
        self.tick_size_mapping = get_binance_tick_size(exchange_info)
        

# %%
def para_allocation(para_dict):
    # 参数排列组合
    allocated_para_detail = list(itertools.product(*para_dict.values()))
    # 参数组匹配参数名
    allocated_para_detail_with_name = list(
        map(lambda x: dict(zip(para_dict.keys(), list(x))), allocated_para_detail))
    return allocated_para_detail_with_name

        