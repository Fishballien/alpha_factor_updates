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
from concurrent.futures import ProcessPoolExecutor, as_completed


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnapsWithTickSize
from core.cache_persist_manager import CacheManager, PersistenceManager
from core.immediate_process_manager import (ImmediateLevelManager, LevelProcessor,
                                            extract_arrays_from_pb_msg)
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        side_type_list = final_factors['side_type']
        amount_type_list = final_factors['amount_type']
        self.cache_mapping = {(side_type, amount_type): f'{side_type}_{amount_type}'
                              for side_type in side_type_list
                              for amount_type in amount_type_list}
            
            
class MyPersistenceMgr(PersistenceManager):
    
    def init_persist_list(self):
        for pr in self.param_set:
            pr_name = pr['name']
            self.persist_list.append(pr_name)
        self.persist_list.append('update_time')
        
        
# %% immediate process
def process_snapshot(*args, multiplier_list, tick_size):
    lp = LevelProcessor(*args)
    lp.load_tick_size(tick_size)
    
    results = {}
    
    # total
    bid_total_amt, ask_total_amt = lp.total_amt_sum['bid'], lp.total_amt_sum['ask']
    bid_amt_ratio = bid_total_amt / (bid_total_amt + ask_total_amt)
    ask_amt_ratio = ask_total_amt / (bid_total_amt + ask_total_amt)
    results[('bid', 'total')] = bid_amt_ratio
    results[('ask', 'total')] = ask_amt_ratio
    
    # if ticktimes
    for multiplier in multiplier_list:
        if_ticktimes_amt = lp.get_if_ticktimes_amt_sum(multiplier)
        bid_amt, ask_amt = if_ticktimes_amt['bid'], if_ticktimes_amt['ask']
        extract_amt = lp.get_extract_ticktimes_amt_sum(multiplier)
        bid_extract_amt, ask_extract_amt = extract_amt['bid'], extract_amt['ask']
        bid_ratio_if_ticktimes = bid_amt / (bid_amt + ask_amt)
        ask_ratio_if_ticktimes = ask_amt / (bid_amt + ask_amt)
        bid_ratio_extract = bid_extract_amt / (bid_extract_amt + ask_extract_amt)
        ask_ratio_extract = ask_extract_amt / (bid_extract_amt + ask_extract_amt)
        
        results[('bid', f'ticktimes{int(multiplier)}')] = bid_ratio_if_ticktimes
        results[('ask', f'ticktimes{int(multiplier)}')] = ask_ratio_if_ticktimes
        results[('bid', f'extract_ticktimes{int(multiplier)}')] = bid_ratio_extract
        results[('ask', f'extract_ticktimes{int(multiplier)}')] = ask_ratio_extract

    return results


class MyImmediateProcessMgr(ImmediateLevelManager):

    def load_info(self, param, tick_size_mapping):
        self.param = param
        self.tick_size_mapping = tick_size_mapping
        
        factors_related = self.param['factors_related']
        cache_factors = factors_related['intermediate']
        self.multiplier_list = cache_factors['multiplier']
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCRngLevel1'] = self._process_cc_level_msg
    
    def get_one_snapshot(self, ts, min_lob):
        with ProcessPoolExecutor(max_workers=5) as executor:
            futures = {}
            for symbol, p in list(min_lob.items()):
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
class F41(FactorUpdaterTsFeatureOfSnapsWithTickSize):
    
    name = 'f41_bora_amount_ratio'
    
    def __init__(self):
        super().__init__()

    def _init_param_names(self):
        for pr in self.param_set:
            side_type = pr['side_type']
            amount_type = pr['amount_type']
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'{side_type}_{amount_type}{suffix}'

    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params, self.tick_size_mapping)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = MyPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    @timeit
    def _final_calc(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            side_type = pr['side_type']
            amount_type = pr['amount_type']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[(side_type, amount_type)]
            if len(factor_per_minute) == 0:
                continue
            if mmt_wd == '0min':
                factor_final = factor_per_minute.iloc[-1]
            else:
                mmt_wd_lookback = self.mmt_wd_lookback_mapping[mmt_wd]
                factor_final = factor_per_minute.loc[ts-mmt_wd_lookback:].mean(axis=0)
            # self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final, ts)
            temp_dict[pr_name] = factor_final
        return temp_dict
    
        
# %%
if __name__=='__main__':
    updater = F41()
    updater.run()
        