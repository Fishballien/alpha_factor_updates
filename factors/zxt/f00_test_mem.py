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


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnaps
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, LevelProcessor
from utils.calc import calc_imb
from utils.decorator_utils import timeit, gc_collect_after


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
    
    # @gc_collect_after
    @timeit
    def _process_cc_level_msg(self, pb_msg):
        lp = LevelProcessor(pb_msg)
        pass
        
# =============================================================================
#         ## general
#         side_amt = lp.side_amt
# 
#         ## small
#         for n in self.n_sigma:
#             lt_n_idx = lp.get_lt_n_sigma_idx(n)
#             bid_lt_amt_sum = np.sum(side_amt['bid'][lt_n_idx['bid']])
#             ask_lt_amt_sum = np.sum(side_amt['ask'][lt_n_idx['ask']])
#             imb_lt = calc_imb(bid_lt_amt_sum, ask_lt_amt_sum)
#             self.factor[n][lp.symbol] = imb_lt
# 
#         self.update_time[lp.symbol] = lp.ts
# =============================================================================
        

# %%
class F00(FactorUpdaterTsFeatureOfSnaps):
    
    name = 'f00_test_mem'
    
    def __init__(self):
        super().__init__()
    
    def _init_param_names(self):
        for pr in self.param_set:
            n_sigma = str(pr['n_sigma']).replace('.', '').replace('-', 'minus')
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'n{n_sigma}{suffix}'

    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)
        
    def _add_tasks(self): # default
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        # self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        # self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
        #                                      self._final_calc_n_send_n_record)
        self.task_scheduler['calc'].add_task("1 Minute Monitor", 'minute', 1, self._monitor_usage)
        
        ## io
        # self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        # self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)
    
    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            n_sigma = pr['n_sigma']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[n_sigma]
            if len(factor_per_minute) == 0:
                continue
            if mmt_wd == '0min':
                factor_final = factor_per_minute.iloc[-1]
            else:
                mmt_wd_lookback = self.mmt_wd_lookback_mapping[mmt_wd]
                factor_final = factor_per_minute.loc[ts-mmt_wd_lookback:].mean(axis=0)
            # self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final)
            temp_dict[pr_name] = factor_final
        return temp_dict

        
# %%
if __name__=='__main__':
    updater = F00()
    updater.run()
        