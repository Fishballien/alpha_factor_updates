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
import sys
from pathlib import Path
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnapsWithTickSize
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import (ImmediateProcessManager, LevelProcessor, Processor,
                                            extract_arrays_from_pb_msg)
from utils.calc import calc_imb
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        amount_type_list = self.params['factors_related']['final']['amount_type']
        self.cache_mapping = {amount_type: f'{amount_type}'
                              for amount_type in amount_type_list}
        
        
# %% immediate process
def process_snapshot(*args, multiplier_list, tick_size):
    lp = LevelProcessor(*args)
    lp.load_tick_size(tick_size)
    
    results = {}
    
    total_amt = lp.total_amt_sum
    imb = calc_imb(total_amt['bid'], total_amt['ask'])
    results['total'] = imb
    
    for multiplier in multiplier_list:
        if_ticktimes_amt = lp.get_if_ticktimes_amt_sum(multiplier)
        extract_amt = lp.get_extract_ticktimes_amt_sum(multiplier)

        imb_if_ticktimes = calc_imb(if_ticktimes_amt['bid'], if_ticktimes_amt['ask'])
        imb_extract = calc_imb(extract_amt['bid'], extract_amt['ask'])
        
        results[f'ticktimes{int(multiplier)}'] = imb_if_ticktimes
        results[f'extract_ticktimes{int(multiplier)}'] = imb_extract

    return results


class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param, tick_size_mapping):
        self.param = param
        self.tick_size_mapping = tick_size_mapping
        
        factors_related = self.param['factors_related']
        cache_factors = factors_related['intermediate']
        self.multiplier_list = cache_factors['multiplier']
    
    def _init_container(self):
        self.container = {}
        self.factor = defaultdict(dict)
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCRngLevel'] = self._process_cc_level_msg
    
    def _process_cc_level_msg(self, pb_msg):
        p = Processor(pb_msg)
        self.container[p.symbol] = p

    def get_one_snapshot(self):
        with ProcessPoolExecutor(max_workers=5) as executor:
            futures = {}
            for symbol, p in list(self.container.items()):
                pb_msg = p.pb_msg
                ts = p.ts
                arrays = extract_arrays_from_pb_msg(pb_msg)
                
                try:
                    tick_size = self.tick_size_mapping[symbol]
                except KeyError:
                    self.log.error(f'Tick size of {symbol} does not exist!')
                    continue
                
                future = executor.submit(process_snapshot, *arrays, 
                                         multiplier_list=self.multiplier_list, 
                                         tick_size=tick_size)
                futures[future] = (symbol, ts)

            for future in as_completed(futures):
                try:
                    results = future.result()
                    symbol, ts = futures[future]

                    for key, imb in results.items():
                        self.factor[key][symbol] = imb
                    
                    self.update_time[symbol] = ts

                except Exception as exc:
                    self.log.error(f"Snapshot processing generated an exception: {exc}")
        

# %%
class F40(FactorUpdaterTsFeatureOfSnapsWithTickSize):
    
    name = 'f40_bidask_amount_ratio'
    
    def __init__(self):
        super().__init__()
        
    def _init_param_names(self):
        for pr in self.param_set:
            amount_type = pr['amount_type']
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'{amount_type}{suffix}'

    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params, self.tick_size_mapping)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            amount_type = pr['amount_type']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[amount_type]
            if len(factor_per_minute) == 0:
                continue
            if mmt_wd == '0min':
                factor_final = factor_per_minute.iloc[-1]
            else:
                mmt_wd_lookback = self.mmt_wd_lookback_mapping[mmt_wd]
                factor_final = factor_per_minute.loc[ts-mmt_wd_lookback:].mean(axis=0)
            self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final)
            temp_dict[pr_name] = factor_final
        return temp_dict

    
# %%
if __name__=='__main__':
    updater = F40()
    updater.run()
        