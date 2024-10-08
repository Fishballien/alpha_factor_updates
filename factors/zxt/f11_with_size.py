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
import traceback
from collections import defaultdict
from datetime import timedelta
import pandas as pd
from collections import Counter
from concurrent.futures import ProcessPoolExecutor, as_completed


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnaps
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, SizeBarProcessor
from utils.calc import calculate_1min_ratio, ts_basic_stat
from utils.decorator_utils import timeit
from utils.timeutils import parse_time_string


# %% cache & persist
class RawMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        side_type_list = final_factors['side_type']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        self.cache_mapping = {(side_type, volume_type, size_div_type, size_type): 
                              '_'.join((side_type, volume_type, size_div_type, size_type))
                              for side_type in side_type_list
                              for volume_type in volume_type_list
                              for size_div_type in size_div_type_list
                              for size_type in size_type_list
                              }
        
        
class RatioMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        side_type_list = final_factors['side_type']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        denominator_wd_list = final_factors['denominator_wd']
        self.cache_mapping = {(side_type, volume_type, size_div_type, size_type, denominator_wd): 
                              '_'.join((side_type, volume_type, size_div_type, size_type, str(denominator_wd)))
                              for side_type in side_type_list
                              for volume_type in volume_type_list
                              for size_div_type in size_div_type_list
                              for size_type in size_type_list
                              for denominator_wd in denominator_wd_list
                              }
        
        
# %% immediate process
class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param):
        self.param = param
        
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.side_type_list = final_factors['side_type']
        self.volume_type_list = final_factors['volume_type']
        self.size_div_type_list = final_factors['size_div_type']
        self.size_type_list = final_factors['size_type']
    
    def _init_container(self):
        self.factor = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self.counter = Counter()
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCSizeBar'] = self._process_cc_size_bar_msg
    
    def _process_cc_size_bar_msg(self, pb_msg):
        sbp = SizeBarProcessor(pb_msg)
        symbol = sbp.symbol
        ts = sbp.timestamp
        ts_in_dt = pd.to_datetime(ts, unit='ms')
        
        for side in self.side_type_list:
            for volume_type in self.volume_type_list:
                for size_div_type in self.size_div_type_list:
                    for size_type in self.size_type_list:
                        self.factor[(side, volume_type, size_div_type, size_type)][ts_in_dt][symbol] = sbp.get(
                            side, volume_type, size=size_type, size_div=size_div_type)

        self.update_time[symbol] = ts
        

# %%
def final_calc_task(pr_name, factor_org, ts, mmt_wd, mmt_wd_lookback, stats_type):
    if mmt_wd == '0min':
        factor_final = factor_org.iloc[-1]
    else:
        factor_final = ts_basic_stat(factor_org, ts, mmt_wd_lookback, stats_type=stats_type)
    return factor_final


class F11(FactorUpdaterTsFeatureOfSnaps):
    
    name = 'f11_with_size'
    
    def __init__(self):
        super().__init__()
    
    def _init_param_names(self):
        for pr in self.param_set:
            side_type = pr['side_type']
            volume_type = pr['volume_type']
            size_div_type = pr['size_div_type']
            size_type = pr['size_type']
            denominator_wd = pr['denominator_wd']
            stats_type = pr['stats_type']
            mmt_wd = pr['mmt_wd']
            suffix = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_{stats_type}'
            pr['name'] = f'{side_type}_{volume_type}_{size_div_type}_{size_type}_d{denominator_wd}min{suffix}'
            
            pr['valid'] = True
            if mmt_wd == '0min' and stats_type != 'avg':
                pr['valid'] = False
                  
    def _init_lookback_mapping(self):
        cache_period = self.params['record']['cache_period']
        mmt_wd_list = self.params['factors_related']['final']['mmt_wd']
        
        self.cache_lookback = timedelta(seconds=parse_time_string(cache_period))
        self.mmt_wd_lookback_mapping = {mmt_wd: timedelta(seconds=parse_time_string(mmt_wd)) 
                                        for mmt_wd in mmt_wd_list}
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params)
        # 定时记录
        self.raw_mgr = RawMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='raw', log=self.log)
        self.ratio_mgr = RatioMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)
        
    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Calc Ratio", 'minute', 30, 
                                             self._calc_ratio)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)

    @timeit
    def _iv_record(self, ts):
        for name, iv_by_ts in list(self.immediate_mgr.factor.items()):
            iv_by_ts = iv_by_ts.copy()
            for ts_of_data, ts_iv in list(iv_by_ts.items()):
                ts_iv_cut = ts_iv.copy()
                self.raw_mgr.add_row(name, ts_iv_cut, ts_of_data)
                for symbol in ts_iv_cut:
                    del self.immediate_mgr.factor[name][ts_of_data][symbol]
            # 删除倒数5行
            del_thres = self.raw_mgr[name].index[-min(len(self.raw_mgr[name]), 5)]
            for ts_of_data in iv_by_ts:
                if ts_of_data < del_thres:
                    assert len(self.immediate_mgr.factor[name][ts_of_data]) == 0
                    del self.immediate_mgr.factor[name][ts_of_data]
                    
    @timeit
    def _calc_ratio(self, ts):
        final_factors = self.params['factors_related']['final']
        side_type_list = final_factors['side_type']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        denominator_wd_list = final_factors['denominator_wd']
        for side_type in side_type_list:
            for volume_type in volume_type_list:
                for size_div_type in size_div_type_list:
                    for size_type in size_type_list:
                        for denominator_wd in denominator_wd_list:
                            factor_per_minute = self.raw_mgr[(side_type, volume_type, size_div_type, size_type)]
                            ratio = calculate_1min_ratio(factor_per_minute, denominator_wd)
                            self.ratio_mgr[(side_type, volume_type, size_div_type, size_type, denominator_wd)] = ratio
       
    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        
        with ProcessPoolExecutor(max_workers=20) as executor:
            futures = {}
            for pr in self.param_set:
                if not pr['valid']:
                    continue
                side_type = pr['side_type']
                volume_type = pr['volume_type']
                size_div_type = pr['size_div_type']
                size_type = pr['size_type']
                denominator_wd = pr['denominator_wd']
                stats_type = pr['stats_type']
                mmt_wd = pr['mmt_wd']
                pr_name = pr['name']
                factor_org = self.ratio_mgr[(side_type, volume_type, size_div_type, size_type, denominator_wd)]
                
                if len(factor_org) == 0:
                    continue
                
                mmt_wd_lookback = self.mmt_wd_lookback_mapping.get(mmt_wd, None)
                future = executor.submit(final_calc_task, pr_name, factor_org, ts, mmt_wd, mmt_wd_lookback, stats_type)
                futures[future] = pr_name
    
            for future in as_completed(futures):
                try:
                    factor_final = future.result()
                    pr_name = futures[future]
    
                    self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final, ts)
                    temp_dict[pr_name] = factor_final
    
                except Exception as exc:
                    self.log.error(f"Error occurred while processing {futures[future]}: {exc}")

        return temp_dict
    
    @timeit
    def _save_to_cache(self, ts):
        self.raw_mgr.save(ts)

        
# %%
if __name__=='__main__':
    updater = F11()
    updater.run()
        