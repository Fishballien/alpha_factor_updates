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
from core.factor_updater import FactorUpdaterWithTickSize
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, LevelProcessor
from utils.timeutils import parse_time_string
from utils.calc import calc_imb
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        amount_type_list = self.params['factors_related']['final']['amount_type']
        self.cache_mapping = {amount_type: f'{amount_type}'
                              for amount_type in amount_type_list}
        
        
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
            print(symbol, list(self.tick_size_mapping.keys()))
            raise
        
        bid_price_arr, ask_price_arr = lp.bid_price, lp.ask_price
        bid_volume_arr, ask_volume_arr = lp.bid_volume, lp.ask_volume
        bid_price_pct, ask_price_pct = lp.get_price_pct()
        
        # total
        bid_total_amt = np.sum(bid_price_arr * bid_volume_arr)
        ask_total_amt = np.sum(ask_price_arr * ask_volume_arr)
        imb = calc_imb(bid_total_amt, ask_total_amt)
        self.factor['total'][lp.symbol] = imb
        
        # if ticktimes
        for multiplier in self.multiplier_list:
            bid_if_ticktimes, ask_if_ticktimes = lp.get_if_ticktimes(tick_size, multiplier)
            bid_amt = np.sum(bid_price_arr[bid_if_ticktimes]
                             * bid_volume_arr[bid_if_ticktimes])
            ask_amt = np.sum(ask_price_arr[ask_if_ticktimes]
                             * ask_volume_arr[ask_if_ticktimes])
            bid_extract_amt = bid_total_amt - bid_amt
            ask_extract_amt = ask_total_amt - ask_amt
            imb_if_ticktimes = calc_imb(bid_amt, ask_amt)
            imb_extract = calc_imb(bid_extract_amt, ask_extract_amt)
            
            self.factor[f'ticktimes{int(multiplier)}'][lp.symbol] = imb_if_ticktimes
            self.factor[f'extract_ticktimes{int(multiplier)}'][lp.symbol] = imb_extract
        
        self.update_time[lp.symbol] = lp.ts
        

# %%
class F40(FactorUpdaterWithTickSize):
    
    name = 'f40_bidask_amount_ratio'
    
    def __init__(self):
        super().__init__()
        
        self._init_param_names()
        self._init_lookback_mapping()
        self._init_task_scheduler()
        self._init_managers()
        self._add_tasks()
        
    def _init_param_names(self):
        for pr in self.param_set:
            amount_type = pr['amount_type']
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'{amount_type}{suffix}'
        
    def _init_lookback_mapping(self):
        cache_period = self.params['record']['cache_period']
        mmt_wd_list = self.params['factors_related']['final']['mmt_wd']
        
        self.cache_lookback = timedelta(seconds=parse_time_string(cache_period))
        self.mmt_wd_lookback_mapping = {mmt_wd: timedelta(seconds=parse_time_string(mmt_wd)) 
                                   for mmt_wd in mmt_wd_list}
        
    def _init_task_scheduler(self):
        self.task_scheduler = {name: TaskScheduler(log=self.log) for name in ['calc', 'io']}
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=None)
        self.immediate_mgr.load_info(self.params, self.tick_size_mapping)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 3, self._minute_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 1, 
                                             self._half_hour_record_n_send)
        self.task_scheduler['calc'].add_task("Reload Tick Size Mapping", 'specific_time', ['00:05'], 
                                     self.reload_tick_size_mapping)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 1, self._minute_save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 1, self._half_hour_save_to_final)
# =============================================================================
#         ## calc
#         self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 1, self._minute_record)
#         self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
#                                              self._half_hour_record_n_send)
#         self.task_scheduler['calc'].add_task("Reload Tick Size Mapping", 'specific_time', ['00:05'], 
#                                      self.reload_tick_size_mapping)
#         
#         ## io
#         self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._minute_save_to_cache)
#         self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._half_hour_save_to_final)
# =============================================================================
        
     
    @timeit
    def _minute_record(self, ts):
        for amount_type, factor_amount_type in list(self.immediate_mgr.factor.items()):
            self.cache_mgr.add_row(amount_type, factor_amount_type, ts)
    
    @timeit
    def _half_hour_record_n_send(self, ts):
        for pr in self.param_set:
            amount_type = pr['amount_type']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[amount_type]
            mmt_wd_lookback = self.mmt_wd_lookback_mapping[mmt_wd]
            factor_ma = factor_per_minute.loc[ts-mmt_wd_lookback:].mean(axis=0)
            self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_ma)
            self.persist_mgr.add_row(pr_name, factor_ma, ts)
            
        self.persist_mgr.add_row('update_time', self.immediate_mgr.update_time, ts)
    
    @timeit
    def _minute_save_to_cache(self, ts):
        self.cache_mgr.save(ts)
    
    @timeit
    def _half_hour_save_to_final(self, ts):
        self.persist_mgr.save(ts)

    def run(self):
        self.msg_controller.start() # 占一条线程，用于收取lord消息存队列
        self.immediate_mgr.start() # 占一条线程，用于即时处理队列消息
        self.task_scheduler['io'].start() # 占2跳线程：处理任务 + 任务调度
        self.task_scheduler['calc'].start(use_thread_for_task_runner=False) # 主线程：处理任务 + 辅助线程：任务调度
        
    def stop(self):
        self.running = False
        self.msg_controller.stop()
        self.immediate_mgr.stop()
        for task_name, task_scheduler in self.task_scheduler.items():
            task_scheduler.stop()
    
        
# %%
if __name__=='__main__':
    updater = F40()
    updater.run()
        