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
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnaps
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, LevelProcessor
from utils.calc import calc_imb
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        n_sigma_list = final_factors['n_sigma']
        n_sigma_names = [str(n).replace('.', '').replace('-', 'minus') for n in n_sigma_list]
        price_range_list = final_factors['price_range']
        price_range_names = [str(pr).replace('.', '') for pr in price_range_list]
        range_type_list = final_factors['range_type']
        self.cache_mapping = {(n, pr, rt): f'rm_lt_{n_name}_{pr_name}_{rt}'
                              for n, n_name in zip(n_sigma_list, n_sigma_names)
                              for pr, pr_name in zip(price_range_list, price_range_names)
                              for rt in range_type_list}
        
        
# %% immediate process
class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param):
        self.param = param
        
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.n_sigma_list = final_factors['n_sigma']
        self.price_range_list = final_factors['price_range']
        self.range_type_list = final_factors['range_type']
    
    def _init_container(self):
        self.factor = defaultdict(dict)
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCLevel'] = self._process_cc_level_msg # !!!: 应该会有新频道名
    
    def _process_cc_level_msg(self, pb_msg):
        lp = LevelProcessor(pb_msg)
        
        ## general
        bid_amt, ask_amt = lp.side_amt

        ## rm out large
        for n in self.n_sigma_list:
            bid_gt_n_idx, ask_gt_n_idx = lp.get_gt_n_sigma_idx(n)
            for pr in self.price_range_list:
                for rt in self.range_type_list:
                    bid_range_idx, ask_range_idx = lp.get_price_range_idx(pr, rt)
                    bid_idx = bid_gt_n_idx | (~bid_range_idx) # ~(lt&in) = gt | ~in
                    ask_idx = ask_gt_n_idx | (~ask_range_idx)
                    bid_amt_sum = np.sum(bid_amt[bid_idx])
                    ask_amt_sum = np.sum(ask_amt[ask_idx])
                    imb = calc_imb(bid_amt_sum, ask_amt_sum)
                    self.factor[(n, pr, rt)][lp.symbol] = imb

        self.update_time[lp.symbol] = lp.ts
        

# %%
class F56(FactorUpdaterTsFeatureOfSnaps):
    
    name = 'f56_ba_amt_ratio_fsmall_by_dist_in'
    
    def _init_param_names(self):
        for pr in self.param_set:
            n_sigma = str(pr['n_sigma']).replace('.', '').replace('-', 'minus')
            price_range = str(pr['price_range']).replace('.', '')
            range_type = pr['range_type']
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'n{n_sigma}_{range_type}_{price_range}{suffix}'
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            n_sigma = pr['n_sigma']
            price_range = pr['price_range']
            range_type = pr['range_type']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[(n_sigma, price_range, range_type)]
            mmt_wd_lookback = self.mmt_wd_lookback_mapping[mmt_wd]
            factor_ma = factor_per_minute.loc[ts-mmt_wd_lookback:].mean(axis=0)
            self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_ma)
            temp_dict[pr_name] = factor_ma
        return temp_dict
    
        
# %%
if __name__=='__main__':
    updater = F56()
    updater.run()
        