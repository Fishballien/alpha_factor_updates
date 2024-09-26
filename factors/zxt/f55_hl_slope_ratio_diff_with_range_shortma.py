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
from utils.calc import calc_imb, compute_slope
from utils.decorator_utils import timeit
from utils.formatters import decimal_to_string


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        x_type_list = final_factors['x_type']
        pct_h_list = final_factors['pct_h']
        pct_h_names = [decimal_to_string(pct_h) for pct_h in pct_h_list]
        pct_l_list = final_factors['pct_l']
        pct_l_names = [decimal_to_string(pct_l) for pct_l in pct_l_list]
        ratio_list = final_factors['ratio']
        ratio_names = [decimal_to_string(ratio) for ratio in ratio_list]
        self.cache_mapping = {(x_type, pct_h, pct_l, ratio): '_'.join((x_type, pct_h_name, pct_l_name, ratio_name))
                              for x_type in x_type_list
                              for pct_h, pct_h_name in zip(pct_h_list, pct_h_names)
                              for pct_l, pct_l_name in zip(pct_l_list, pct_l_names)
                              for ratio, ratio_name in zip(ratio_list, ratio_names)
                              }
        
        
# %% immediate process
def process_snapshot(*args, x_type_list, pct_h_list, pct_l_list, ratio_list, tick_size):
    lp = LevelProcessor(*args)
    lp.load_tick_size(tick_size)
    
    results = {}
    
    x_by_level = {
        'pct': lp.prices_pct_by_level,
        'layer': lp.prices_layer_by_level,
        'tick': lp.prices_tick_by_level,
    }
    amt_cum_ratio_sorted_by_level = lp.amt_cum_ratio_sorted_by_level
    
    for x_type in x_type_list:
        x = x_by_level[x_type]
        for pct_h in pct_h_list:
            h_idx = lp.get_range_idx_on_sorted(lt=pct_h)
            h_slope = {side: compute_slope(x[side][h_idx[side]], 
                                           amt_cum_ratio_sorted_by_level[side][h_idx[side]])
                       for side in ('bid', 'ask')}
            h_imb = calc_imb(h_slope['bid'], h_slope['ask'])
            for pct_l in pct_l_list:
                l_idx = lp.get_range_idx_on_sorted(gt=pct_h, lt=pct_l)
                l_slope = {side: compute_slope(x[side][l_idx[side]], 
                                               amt_cum_ratio_sorted_by_level[side][l_idx[side]])
                           for side in ('bid', 'ask')}
                l_imb = calc_imb(l_slope['bid'], l_slope['ask'])
                for ratio in ratio_list:
                    imb_diff = h_imb - ratio * l_imb
                    results[(x_type, pct_h, pct_l, ratio)] = imb_diff

    return results


class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param, tick_size_mapping):
        self.param = param
        self.tick_size_mapping = tick_size_mapping
        
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.x_type_list = final_factors['x_type']
        self.pct_h_list = final_factors['pct_h']
        self.pct_l_list = final_factors['pct_l']
        self.ratio_list = final_factors['ratio']
    
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
                                         x_type_list=self.x_type_list,
                                         pct_h_list=self.pct_h_list,
                                         pct_l_list=self.pct_l_list,
                                         ratio_list=self.ratio_list,
                                         tick_size=tick_size)
                futures[future] = (symbol, ts)

            for future in as_completed(futures):
                try:
                    results = future.result()
                    symbol, ts = futures[future]

                    for key, imb_diff in results.items():
                        self.factor[key][symbol] = imb_diff

                    self.update_time[symbol] = ts

                except Exception as exc:
                    self.log.error(f"Snapshot processing generated an exception: {exc}")
        

# %%
class F55(FactorUpdaterTsFeatureOfSnapsWithTickSize):
    
    name = 'f55_hl_slope_ratio_diff_with_range_shortma'
    
    def __init__(self):
        super().__init__()
    
    def _init_param_names(self):
        for pr in self.param_set:
            x_type = pr['x_type']
            pct_h = decimal_to_string(pr['pct_h'])
            pct_l = decimal_to_string(pr['pct_l'])
            ratio = decimal_to_string(pr['ratio'])
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_ma'
            pr['name'] = f'x{x_type}_h{pct_h}_l{pct_l}_r{ratio}{suffix}'
        
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
            x_type = pr['x_type']
            pct_h = pr['pct_h']
            pct_l = pr['pct_l']
            ratio = pr['ratio']
            mmt_wd = pr['mmt_wd']
            pr_name = pr['name']
            factor_per_minute = self.cache_mgr[(x_type, pct_h, pct_l, ratio)]
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
    updater = F55()
    updater.run()
        