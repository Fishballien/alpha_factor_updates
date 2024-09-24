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
import h5py
import numpy as np
import threading


from utils.timeutils import get_curr_utc_date, get_date_based_on_timestamp
from utils.datautils import add_row_to_dataframe_reindex


# %%
class DataManager(ABC):
    
    def __init__(self, params, param_set, log=None):
        self.params = params
        self.param_set = param_set
        self.log = log
        
        self._init_locks()
        
    def _init_locks(self):
        self.locks = defaultdict(threading.Lock)

    def _load_or_init_batch(self, path, container, keys, type_name='Path'):
        if not path.exists():
            return
        try:
            self._load_from_h5_batch(path, container, keys)
            if self.log:
                self.log.success(f'Successfully loaded {type_name} from {path}')
        except Exception as e:
            if self.log:
                self.log.warning(f'{type_name} exists but unreadable: {path}. Error: {e}')

    def _save_to_h5_batch(self, path, container):
        with h5py.File(path, 'w') as f:
            for key, data in list(container.items()):
                dset = f.create_dataset(key, data=data.to_numpy(dtype=np.float64))
                # 如果 index 是时间戳，转换为字符串保存
                if isinstance(data.index, pd.DatetimeIndex):
                    f[key].attrs['index'] = [i.isoformat() for i in data.index]
                else:
                    f[key].attrs['index'] = [i.encode('utf-8') if isinstance(i, str) else i for i in data.index]
                f[key].attrs['columns'] = [col.encode('utf-8') for col in data.columns.astype(str)]
        if self.log:
            self.log.success(f'Successfully saved data to {path}')

    def _load_from_h5_batch(self, path, container, keys):
        loaded_info = {}
        with h5py.File(path, 'r') as f:
            for key in keys:
                if key in f:
                    data = np.array(f[key])
                    # 判断 index 是否为时间戳字符串格式
                    index = [
                        pd.Timestamp(i) if isinstance(i, str) and "T" in i else i.decode('utf-8') if isinstance(i, bytes) else i
                        for i in f[key].attrs['index']
                    ]
                    columns = [
                        col.decode('utf-8') if isinstance(col, bytes) else col for col in f[key].attrs['columns']
                    ]
                    container[key] = pd.DataFrame(data, index=index, columns=columns)
                    loaded_info[key] = data.shape
        if self.log:
            self.log.success(f'Loaded details: {loaded_info}')

    def _add_row(self, container, key, new_row, index):
        container[key] = add_row_to_dataframe_reindex(container[key], new_row, index)


class CacheManager(DataManager):
    
    def __init__(self, params, param_set, cache_dir, cache_lookback, file_name=None, log=None):
        super().__init__(params, param_set, log=log)
        self.cache_dir = cache_dir  # 确保 cache_dir 是 Path 对象
        self.cache_lookback = cache_lookback
        self.file_name = file_name  # 通过参数传入文件名
        self.cache_mapping = {}

        self._init_containers()
        self._init_cache_mapping()
        self._load_or_init_cache()
        
    def _init_containers(self):
        self.cache = defaultdict(pd.DataFrame)
        
    def _init_cache_mapping(self):
        self.init_cache_mapping()
        if self.cache_mapping:
            self.log.success(f'Cache Mapping Registered: {self.cache_mapping}')

    @abstractmethod
    def init_cache_mapping(self):
        # 子类必须实现
        pass
        
    def _load_or_init_cache(self):
        if self.file_name is None:
            return  # 如果没有自定义文件名，跳过加载
        path = self.cache_dir / f'{self.file_name}.h5'  # 使用斜杠连接路径
        save_names = self.cache_mapping.values()  # 使用保存名进行加载
        self._load_or_init_batch(path, self.cache, save_names, type_name='Cache')

    def _cut_cache(self, save_name, curr_ts):
        cut_time = pd.Timestamp(curr_ts-self.cache_lookback)
        self.cache[save_name] = self.cache[save_name].loc[cut_time:]
            
    def save(self, ts):
        if self.file_name is None:
            return  # 如果没有自定义文件名，跳过保存
        for save_name in self.cache_mapping.values():  # 使用保存名进行操作
            with self.locks[save_name]:
                self._cut_cache(save_name, ts)  # 保留 cut_cache 步骤
        
        # 批量保存到 HDF5 文件，直接传 self.cache 并使用保存名
        path = self.cache_dir / f'{self.file_name}.h5'
        self._save_to_h5_batch(path, self.cache)
            
    def add_row(self, access_name, new_row, index):
        save_name = self.cache_mapping[access_name]
        with self.locks[save_name]:
            self._add_row(self.cache, save_name, new_row, index)
        
    def __getitem__(self, access_name):
        save_name = self.cache_mapping[access_name]
        return self.cache[save_name]
    
    def __setitem__(self, access_name, data):
        save_name = self.cache_mapping[access_name]
        self.cache[save_name] = data


class PersistenceManager(DataManager):
    
    def __init__(self, params, param_set, persist_dir, log=None):
        super().__init__(params, param_set, log=log)
        self.persist_dir = persist_dir  # 确保 persist_dir 是 Path 对象
        self.persist_list = []
        
        self._init_containers()
        self._init_persist_list()
        self._load_or_init_persist()
        
    def _init_containers(self):
        self.factor_persist = defaultdict(pd.DataFrame)
        
    def _init_persist_list(self):
        self.init_persist_list()
        if self.persist_list:
            self.log.success(f'Persist List Registered: {self.persist_list}')

    @abstractmethod
    def init_persist_list(self):
        # 子类必须实现
        pass
        
    def _load_or_init_persist(self):
        date_to_load = get_curr_utc_date()  # 根据当前日期生成文件名
        path = self.persist_dir / f'{date_to_load}.h5'
        self._load_or_init_batch(path, self.factor_persist, self.persist_list, type_name='Persist')
            
    def save(self, ts):
        date_to_save = get_date_based_on_timestamp(ts)  # 根据传入时间戳生成日期
        for key in self.persist_list:
            with self.locks[key]:
                self.factor_persist[key] = self.factor_persist[key].loc[date_to_save:]  # 保存当天数据
        
        path = self.persist_dir / f'{date_to_save}.h5'
        self._save_to_h5_batch(path, self.factor_persist)
            
    def add_row(self, key, new_row, index):
        with self.locks[key]:
            self._add_row(self.factor_persist, key, new_row, index)
        
        
# %%
class GeneralPersistenceMgr(PersistenceManager):
    
    def init_persist_list(self):
        for pr in self.param_set:
            pr_name = pr['name']
            self.persist_list.append(pr_name)
        self.persist_list.append('update_time')