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
import pandas as pd
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
from core.immediate_process_manager import (ImmediateLevelManager, LevelProcessor, Processor,
                                            extract_arrays_from_pb_msg)
from utils.calc import calc_imb
from utils.decorator_utils import timeit
from utils.timeutils import round_up_timestamp


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
        bid_lt_amt_sum = np.sum(side_amt['bid'][lt_n_idx['bid']])
        ask_lt_amt_sum = np.sum(side_amt['ask'][lt_n_idx['ask']])
        imb_lt = calc_imb(bid_lt_amt_sum, ask_lt_amt_sum)
        results.append((n, imb_lt))
        
    return results


class MyImmediateProcessMgr(ImmediateLevelManager):

    def load_info(self, param):
        self.param = param
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.n_sigma = final_factors['n_sigma']
        
        import json
        from datetime import datetime, timedelta
        cooling_off_period = 15
        ex_path = r'D:/mnt/Data/Crypto/Tradable/exchange_info_usd.json'
        with open(ex_path, 'rb') as f:
            exchange_info = json.load(f)
        today = datetime.now()
        cooling_delta = timedelta(days=cooling_off_period)
        ipo_before_thres = today - cooling_delta
        trading_symbols = [symbol_info['symbol'].lower() for symbol_info in exchange_info['symbols']
                           if (
                                   symbol_info['status'] == 'TRADING' 
                                   and symbol_info['symbol'].endswith('USDT')
                                   # and pd.to_datetime(symbol_info['onboardDate'], unit='ms') < ipo_before_thres
                               )]
        self.trading_symbols = sorted(trading_symbols)

    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCRngLevel1'] = self._process_cc_level_msg
    
    def get_one_snapshot(self, ts, min_lob):
        missing = [symbol for symbol in self.trading_symbols if symbol not in min_lob]
        more = [symbol for symbol in min_lob if symbol not in self.trading_symbols]
        print(f'trading: {len(self.trading_symbols)} rcv: {len(min_lob)} missing: {missing} more: {more}')
        with ProcessPoolExecutor(max_workers=5) as executor:
            futures = {}
            for symbol, p in list(min_lob.items()):
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
    
    name = 'f39_test'
    
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
    updater = F39()
    updater.run()
        