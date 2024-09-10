# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 15:35:32 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
import os
import pandas as pd
from collections import defaultdict
from abc import ABC, abstractmethod


from utils.timeutils import get_curr_utc_date, get_date_based_on_timestamp
from utils.datautils import add_row_to_dataframe_reindex


# %%
class DataManager(ABC):
    
    def __init__(self, params, param_set, log=None):
        self.params = params
        self.param_set = param_set
        self.log = log

    def _load_or_init(self, path, container, key, type_name='Path'):
        if not os.path.exists(path):
            return
        try:
            container[key] = pd.read_parquet(path)
        except:
            self.log.warning(f'{type_name} exists but unreadable: {path}')

    def _save(self, path, data):
        data.to_parquet(path)
        
    def _add_row(self, container, key, new_row, index):
        container[key] = add_row_to_dataframe_reindex(container[key], new_row, index)


class CacheManager(DataManager):
    
    def __init__(self, params, param_set, cache_dir, cache_lookback, log=None):
        super().__init__(params, param_set, log=log)
        self.cache_dir = cache_dir
        self.cache_lookback = cache_lookback
        self.cache_mapping = defaultdict(dict)
        self._init_containers()
        self._init_cache_mapping()
        self._load_or_init_cache()
        
    def _init_containers(self):
        self.cache = defaultdict(pd.DataFrame)
    
    @abstractmethod
    def _init_cache_mapping(self):
        pass
        
    def _load_or_init_cache(self):
        for name, info in self.cache_mapping.items():
            path = info['path']
            container = info['container']
            key = info['key']
            self._load_or_init(path, container, key, type_name='Cache')
            
    def _cut_cache(self, container, key, curr_ts):
        container[key] = container[key].loc[curr_ts-self.cache_lookback:]
            
    def save(self, ts):
        for name, info in self.cache_mapping.items():
            path = info['path']
            container = info['container']
            key = info['key']
            self._cut_cache(container, key, ts)
            self._save(path, container[key])
            
    def add_row(self, mapping_key, new_row, index):
        container = self.cache_mapping[mapping_key]['container']
        key = self.cache_mapping[mapping_key]['key']
        self._add_row(container, key, new_row, index)
            
            
class PersistenceManager(DataManager):
    
    def __init__(self, params, param_set, persist_dir, log=None):
        super().__init__(params, param_set, log=log)
        self.persist_dir = persist_dir
        self.persist_mapping = defaultdict(dict)
        self._init_containers()
        self._init_persist_mapping()
        self._load_or_init_persist()
        
    def _init_containers(self):
        self.factor_persist = defaultdict(pd.DataFrame)
    
    @abstractmethod
    def _init_persist_mapping(self):
        pass
        
    def _load_or_init_persist(self):
        date_to_load = get_curr_utc_date()
        for key, info in self.persist_mapping.items():
            dir_ = info['dir']
            container = info['container']
            path = dir_ / f'{date_to_load}.parquet'
            self._load_or_init(path, container, key, type_name='persist')
            
    def save(self, ts):
        date_to_save = get_date_based_on_timestamp(ts)
        for name, info in self.persist_mapping.items():
            dir_ = info['dir']
            container = info['container']
            key = info['key']
            container[key] = container[key].loc[date_to_save:] # 从UTC时间今天00:00后的数据开始
            path = dir_ / f'{date_to_save}.parquet'
            self._save(path, container[key])
            
    def add_row(self, mapping_key, new_row, index):
        container = self.persist_mapping[mapping_key]['container']
        key = self.persist_mapping[mapping_key]['key']
        self._add_row(container, key, new_row, index)