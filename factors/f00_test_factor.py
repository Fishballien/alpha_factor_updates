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
    1. signal
    2. cache & final
    3. 重启读cache & final
    4. container另起模块
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
from core.factor_updater import FactorUpdater
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, PersistenceManager
from core.immediate_process_manager import ImmediateProcessManager
from utils.data_parser import convert_to_lowercase
from utils.timeutils import parse_time_string
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def _init_cache_mapping(self):
        amount_type_list = self.params['factors_related']['final']['amount_type']
        for amount_type in amount_type_list:
            cache_path = self.cache_dir / f'cache_1min_{amount_type}.parquet'
            self.cache_mapping[amount_type]['path'] = cache_path
            self.cache_mapping[amount_type]['container'] = self.cache
            self.cache_mapping[amount_type]['key'] = amount_type # 和mapping的键值一样，但若有多个容器，就可能不同
            
            
class MyPersistenceMgr(PersistenceManager):
    
    def _init_persist_mapping(self):
        for pr in self.param_set:
            pr_name = pr['name']
            pr_persist_dir = self.persist_dir / pr_name
            pr_persist_dir.mkdir(parents=True, exist_ok=True)
            self.persist_mapping[pr_name]['dir'] = pr_persist_dir
            self.persist_mapping[pr_name]['container'] = self.factor_persist
            self.persist_mapping[pr_name]['key'] = pr_name
        ts_persist_dir = self.persist_dir / 'update_time'
        ts_persist_dir.mkdir(parents=True, exist_ok=True)
        self.persist_mapping['update_time']['dir'] = ts_persist_dir
        self.persist_mapping['update_time']['container'] = self.factor_persist
        self.persist_mapping['update_time']['key'] = 'update_time'
        
        
# %% immediate process
class MyImmediateProcessMgr(ImmediateProcessManager):
    
    # 此处未定义__init__，实际使用时，若即时计算需要用到某些参数，可以设置__init__传入
    
    def _init_container(self):
        self.factor = defaultdict(dict)
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCLevel'] = self._process_cc_level_msg
        
    def _process_cc_level_msg(self, pb_msg):
        header = pb_msg.header
        symbol = convert_to_lowercase(header.symbol)
        ts = header.timestamp
        bid_info, ask_info = pb_msg.bid, pb_msg.ask
        
        bid_price_arr = np.array(bid_info.price)
        try:
            assert len(bid_price_arr) > 0
        except AssertionError:
            self.log.error('Empty bid price arr for {symbol}!')
            traceback.print_exc()
        bid_volume_arr = np.array(bid_info.volume)
        bid_level_arr = np.array(bid_info.level)
        try:
            assert all(np.diff(bid_level_arr) > 0)
        except AssertionError:
            traceback.print_exc()
        
        ask_price_arr = np.array(ask_info.price)
        try:
            assert len(ask_price_arr) > 0
        except AssertionError:
            self.log.error('Empty ask price arr for {symbol}!')
            traceback.print_exc()
        ask_volume_arr = np.array(ask_info.volume)
        ask_level_arr = np.array(ask_info.level)
        try:
            assert all(np.diff(ask_level_arr) < 0)
        except AssertionError:
            traceback.print_exc()
        
        bp1 = bid_price_arr[0]
        sp1 = ask_price_arr[-1]
        mpc = (bp1 + sp1) / 2
        bid_price_pct = (mpc - bid_price_arr) / mpc
        ask_price_pct = (ask_price_arr - mpc) / mpc
        valid_bid_range = bid_price_pct < 0.2
        valid_ask_range = ask_price_pct < 0.2
        bid_total_amt = np.sum(bid_price_arr[valid_bid_range]
                               * bid_volume_arr[valid_bid_range])
        ask_total_amt = np.sum(ask_price_arr[valid_ask_range]
                               * ask_volume_arr[valid_ask_range])
        imb = (bid_total_amt - ask_total_amt) / (bid_total_amt + ask_total_amt)
        
        self.factor['total'][symbol] = imb
        self.update_time[symbol] = ts
        

# %%
class F00TestFactor(FactorUpdater):
    
    name = 'f00_test_factor'
    
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
            pr['name'] = f'{amount_type}_mmt{mmt_wd}_ma'
        
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
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                  self.cache_lookback, log=self.log)
        self.persist_mgr = MyPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 3, self._minute_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 1, self._half_hour_record_n_send)
        
        ## io
        self.task_scheduler['io'].add_task("1 Minute Save to Cache", 'minute', 1, self._minute_save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 1, self._half_hour_save_to_final)
    
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
            factor_per_minute = self.cache_mgr.cache[amount_type]
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
    test = F00TestFactor()
    test.run()
        