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
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnaps
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import (ImmediateProcessManager, LevelProcessor, Processor,
                                            extract_arrays_from_pb_msg)
from utils.calc import calc_imb
from utils.decorator_utils import timeit


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        n_sigma_list = self.params['factors_related']['final']['n_sigma']
        n_sigma_names = [str(n).replace('.', '').replace('-', 'minus') for n in n_sigma_list]
        self.cache_mapping = {n: n_name for n, n_name in zip(n_sigma_list, n_sigma_names)}
        
        
# %% immediate process
def process_snapshot(*args, n_sigma):
    lp = LevelProcessor(*args)
    
    results = []
    
    side_amt = lp.side_amt
    
    for n in n_sigma:
        lt_n_idx = lp.get_lt_n_sigma_idx(n)
        thres = lp.get_n_sigma_thres(n)
        bid_lt_amt_sum = np.sum(side_amt['bid'][lt_n_idx['bid']])
        ask_lt_amt_sum = np.sum(side_amt['ask'][lt_n_idx['ask']])
        imb_lt = calc_imb(bid_lt_amt_sum, ask_lt_amt_sum)
        results.append((n, imb_lt, bid_lt_amt_sum, ask_lt_amt_sum, thres))
        
    return results


class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param):
        self.param = param
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.n_sigma = final_factors['n_sigma']
    
    def _init_container(self):
        self.container = {}
        self.factor = defaultdict(dict)
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCRngLevel'] = self._process_cc_level_msg
    
    def _process_cc_level_msg(self, pb_msg):
        p = Processor(pb_msg)
        self.container[p.symbol] = p  # 只接收并存储到 container 中
    
    def get_one_snapshot(self):
        with ProcessPoolExecutor(max_workers=5) as executor:
            futures = {}
            for symbol, p in list(self.container.items()):
                pb_msg = p.pb_msg
                ts = p.ts
                arrays = extract_arrays_from_pb_msg(pb_msg)

                future = executor.submit(process_snapshot, *arrays, n_sigma=self.n_sigma)
                futures[future] = (symbol, ts)

            for future in as_completed(futures):
                try:
                    results = future.result()
                    symbol, ts = futures[future]

                    for n, imb_lt in results:
                        self.factor[n][symbol] = imb_lt
                    
                    self.update_time[symbol] = ts

                except Exception as exc:
                    self.log.error(f"Snapshot processing generated an exception: {exc}")


# %%
class F39(FactorUpdaterTsFeatureOfSnaps):
    
    name = 'f39_small_ba_amt_ratio'
    
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
    
    @timeit
    def _final_calc(self, ts):
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
            # self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final, ts)
            temp_dict[pr_name] = factor_final
        return temp_dict

        
# %%
if __name__=='__main__':
    import pandas as pd
    
    n_sigma = [-3.0, -2.0, -1.0, 
    0.1, 0.25, 0.5, 1.0, 2.0, 3.0, 4.0, 5.0, 
    7.5, 10.0, 15.0, 20.0, 25.0]
    
    save_dir = Path(r'D:\crypto\DataProcessing\lob_shape\sample_data\compare\yl')
    name = '0.parquet'
    lob_path = save_dir / name
    raw_lob = pd.read_parquet(lob_path)
    bid_side_idx = raw_lob['lob_bid'] > 0
    ask_side_idx = raw_lob['lob_ask'] > 0
    bid_lob = raw_lob[bid_side_idx]
    ask_lob = raw_lob[ask_side_idx]
    bid_price_arr = bid_lob['price'][::-1]
    bid_volume_arr = bid_lob['lob_bid'][::-1]
    bid_level_arr = np.arange(1, len(bid_lob)+1, 1)
    ask_price_arr = ask_lob['price']
    ask_volume_arr = ask_lob['lob_ask']
    ask_level_arr = np.arange(1, len(ask_lob)+1, 1)
    
    res = process_snapshot(*(bid_price_arr, bid_volume_arr, bid_level_arr, 
                             ask_price_arr, ask_volume_arr, ask_level_arr), n_sigma=n_sigma)
    indv1 = pd.read_parquet(r'D:\crypto\prod\alpha\factors_update\verify\sample_data\indv1.parquet')
    indv4 = pd.read_parquet(r'D:\crypto\prod\alpha\factors_update\verify\sample_data\indv4.parquet')
    n_sigma_name = '01'
    bid_lt = indv1['bid_total_amount'] - indv4['bid_amt_gt_sigma_01']
    ask_lt = indv1['ask_total_amount'] - indv4['ask_amt_gt_sigma_01']
    imb = (bid_lt - ask_lt) / (bid_lt + ask_lt)
    print(bid_lt.iloc[0], ask_lt.iloc[0], imb.iloc[0])
    print(res[3])
    
    curr_dataset = np.zeros((len(n_sigma), 3))
    MINIMUM_SIZE_FILTER = 1e-8
    lob_bid = raw_lob['lob_bid']
    lob_ask = raw_lob['lob_ask']
    prices = raw_lob['price']
    lob_all = lob_bid + lob_ask
    lob_valid_idx = lob_all > MINIMUM_SIZE_FILTER
    bid_amt = lob_bid * prices
    ask_amt = lob_ask * prices
    all_amt = lob_all * prices
    mean = np.median(all_amt[lob_valid_idx])
    std = np.std(all_amt[lob_valid_idx])
    
    for i_n, n in enumerate(n_sigma):
        thres = mean + n * std
        curr_dataset[i_n, 0] = np.sum(bid_amt[bid_amt > thres])
        curr_dataset[i_n, 1] = np.sum(ask_amt[ask_amt > thres])
        curr_dataset[i_n, 2] = thres
        
    bid_total_amount = np.sum(bid_amt)
    ask_total_amount = np.sum(ask_amt)
    bid_lt_bt = - curr_dataset[:, 0] + bid_total_amount
    ask_lt_bt = - curr_dataset[:, 1] + ask_total_amount
    
    '''
    241207发现问题：gt/lt n_sigma的thres应该使用all_amt_median而不是mean，修复后完全对上
    6765846.436501183 5061760.799399 0.14407695513677474
    (0.1, 0.1440769551365924, 6765846.4365, 5061760.7994, 15144.900725076463)
    '''