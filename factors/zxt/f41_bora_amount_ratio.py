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
# %%
'''
TODOs:
    1. 改ticksize相关
'''
# %% imports
import sys
from pathlib import Path
import numpy as np
import traceback
from collections import defaultdict
from datetime import timedelta


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterWithTickSize, FactorUpdaterTsFeatureOfSnaps
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, PersistenceManager
from core.immediate_process_manager import ImmediateProcessManager, LevelProcessor
from utils.timeutils import parse_time_string
from utils.calc import calc_imb
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
class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param, tick_size_mapping):
        self.param = param
        self.tick_size_mapping = tick_size_mapping
        
        factors_related = self.param['factors_related']
        cache_factors = factors_related['intermediate']
        self.multiplier_list = cache_factors['multiplier']
    
    def _init_container(self):
        self.factor = defaultdict(dict)
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCLevel'] = self._process_cc_level_msg # !!!: 应该会有新频道名
    
    # @timeit
    def _process_cc_level_msg(self, pb_msg):
        lp = LevelProcessor(pb_msg)
        symbol = lp.symbol
        try:
            tick_size = self.tick_size_mapping[symbol]
        except:
            self.log.error(f'Tick size of {symbol} does not exist!')
        lp.load_tick_size(tick_size)

        # total
        bid_total_amt, ask_total_amt = lp.total_amt_sum['bid'], lp.total_amt_sum['ask']
        bid_amt_ratio = bid_total_amt / (bid_total_amt + ask_total_amt)
        ask_amt_ratio = ask_total_amt / (bid_total_amt + ask_total_amt)
        self.factor[('bid', 'total')][lp.symbol] = bid_amt_ratio
        self.factor[('ask', 'total')][lp.symbol] = ask_amt_ratio
        
        # if ticktimes
        for multiplier in self.multiplier_list:
            if_ticktimes_amt = lp.get_if_ticktimes_amt_sum(multiplier)
            bid_amt, ask_amt = if_ticktimes_amt['bid'], if_ticktimes_amt['ask']
            extract_amt = lp.get_extract_ticktimes_amt_sum(multiplier)
            bid_extract_amt, ask_extract_amt = extract_amt['bid'], extract_amt['ask']
            bid_ratio_if_ticktimes = bid_amt / (bid_amt + ask_amt)
            ask_ratio_if_ticktimes = ask_amt / (bid_amt + ask_amt)
            bid_ratio_extract = bid_extract_amt / (bid_extract_amt + ask_extract_amt)
            ask_ratio_extract = ask_extract_amt / (bid_extract_amt + ask_extract_amt)
            
            self.factor[('bid', f'ticktimes{int(multiplier)}')][lp.symbol] = bid_ratio_if_ticktimes
            self.factor[('ask', f'ticktimes{int(multiplier)}')][lp.symbol] = ask_ratio_if_ticktimes
            self.factor[('bid', f'extract_ticktimes{int(multiplier)}')][lp.symbol] = bid_ratio_extract
            self.factor[('ask', f'extract_ticktimes{int(multiplier)}')][lp.symbol] = ask_ratio_extract
        
        self.update_time[lp.symbol] = lp.ts
        

# %%
class F41(FactorUpdaterTsFeatureOfSnaps, FactorUpdaterWithTickSize):
    
    name = 'f41_bora_amount_ratio'
    
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
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=None)
        self.immediate_mgr.load_info(self.params, self.tick_size_mapping)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = MyPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        self.task_scheduler['calc'].add_task("Reload Tick Size Mapping", 'specific_time', ['00:05'], 
                                     self.reload_tick_size_mapping)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)

    @timeit
    def _final_calc_n_send(self, ts):
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
            self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final)
            temp_dict[pr_name] = factor_final
        return temp_dict
    
        
# %%
if __name__=='__main__':
    updater = F41()
    updater.run()
        