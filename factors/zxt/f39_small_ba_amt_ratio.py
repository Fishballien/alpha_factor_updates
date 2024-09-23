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
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, LevelProcessor
from utils.timeutils import parse_time_string
from utils.calc import calc_imb
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        n_sigma_list = self.params['factors_related']['final']['n_sigma']
        n_sigma_names = [str(n).replace('.', '').replace('-', 'minus') for n in n_sigma_list]
        self.cache_mapping = {n: n_name for n, n_name in zip(n_sigma_list, n_sigma_names)}
        
        
# %% immediate process
class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param):
        self.param = param
        
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.n_sigma = final_factors['n_sigma']
    
    def _init_container(self):
        self.factor = defaultdict(dict)
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCLevel'] = self._process_cc_level_msg # !!!: 应该会有新频道名
    
    def _process_cc_level_msg(self, pb_msg):
        lp = LevelProcessor(pb_msg)
        
        ## general
        bid_amt, ask_amt = lp.side_amt

        ## small
        for n in self.n_sigma:
            bid_lt_n_idx, ask_lt_n_idx = lp.get_lt_n_sigma_idx(n)
            bid_lt_amt_sum = np.sum(bid_amt[bid_lt_n_idx])
            ask_lt_amt_sum = np.sum(ask_amt[ask_lt_n_idx])
            imb_lt = calc_imb(bid_lt_amt_sum, ask_lt_amt_sum)
            self.factor[n][lp.symbol] = imb_lt

        self.update_time[lp.symbol] = lp.ts
        

# %%
class F39(FactorUpdater):
    
    name = 'f39_small_ba_amt_ratio'
    
    def __init__(self):
        super().__init__()
        
        self._init_param_names()
        self._init_lookback_mapping()
        self._init_task_scheduler()
        self._init_managers()
        self._add_tasks()
        
    def _init_param_names(self):
        for pr in self.param_set:
            n_sigma = str(pr['n_sigma']).replace('.', '').replace('-', 'minus')
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'n{n_sigma}{suffix}'
        
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
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 3, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 1, 
                                             self._final_calc_n_send_n_record)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 1, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 1, self._save_to_final)
# =============================================================================
#         ## calc
#         self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 1, self._iv_record)
#         self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
#                                              self._final_calc_n_send_n_record)
#         
#         ## io
#         self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
#         self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)
# =============================================================================
        
    @timeit
    def _iv_record(self, ts):
        for name, iv in list(self.immediate_mgr.factor.items()):
            self.cache_mgr.add_row(name, iv, ts)
    
    def _final_calc_n_send_n_record(self, ts):
        temp_dict = self._final_calc_n_send(ts)
        self._final_record(ts, temp_dict)
    
    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            n_sigma = pr['n_sigma']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[n_sigma]
            mmt_wd_lookback = self.mmt_wd_lookback_mapping[mmt_wd]
            factor_ma = factor_per_minute.loc[ts-mmt_wd_lookback:].mean(axis=0)
            self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_ma)
            temp_dict[pr_name] = factor_ma
        return temp_dict
    
    @timeit
    def _final_record(self, ts, temp_dict):
        for pr_name, factor_ma in temp_dict.items():
            self.persist_mgr.add_row(pr_name, factor_ma, ts)
            
        self.persist_mgr.add_row('update_time', self.immediate_mgr.update_time, ts)
    
    @timeit
    def _save_to_cache(self, ts):
        self.cache_mgr.save(ts)
    
    @timeit
    def _save_to_final(self, ts):
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
    updater = F39()
    updater.run()
        