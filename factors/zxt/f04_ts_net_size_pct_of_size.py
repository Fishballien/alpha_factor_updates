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
import pandas as pd
from collections import Counter


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[1]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnaps
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, SizeBarProcessor, BarProcessor
from utils.calc import safe_ts_regress_once, ts_basic_stat
from utils.decorator_utils import timeit
from utils.timeutils import parse_time_string
from utils.datautils import is_empty_dict


# %% cache & persist
class RawMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        self.cache_mapping = {(volume_type, size_div_type, size_type): 
                              '_'.join((volume_type, size_div_type, size_type))
                              for volume_type in volume_type_list
                              for size_div_type in size_div_type_list
                              for size_type in size_type_list
                              }
        self.cache_mapping.update({'rtn': 'rtn'})
        
        
class RegMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        reg_type_list = final_factors['reg_type']
        lookback_wd_list = final_factors['lookback_wd']
        self.cache_mapping = {(volume_type, size_div_type, size_type, reg_type, lookback_wd): 
                              '_'.join((volume_type, size_div_type, size_type, reg_type, lookback_wd))
                              for volume_type in volume_type_list
                              for size_div_type in size_div_type_list
                              for size_type in size_type_list
                              for reg_type in reg_type_list
                              for lookback_wd in lookback_wd_list
                              }
            
            
class RegStatsMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['final']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        reg_type_list = final_factors['reg_type']
        lookback_wd_list = final_factors['lookback_wd']
        mmt_wd_list = final_factors['mmt_wd']
        stats_type_list = final_factors['stats_type']
        self.cache_mapping = {(volume_type, size_div_type, size_type, reg_type, lookback_wd, mmt_wd, stats_type): 
                              '_'.join((volume_type, size_div_type, size_type, reg_type, 
                                        lookback_wd, mmt_wd , stats_type))
                              for volume_type in volume_type_list
                              for size_div_type in size_div_type_list
                              for size_type in size_type_list
                              for reg_type in reg_type_list
                              for lookback_wd in lookback_wd_list
                              for mmt_wd in mmt_wd_list
                              for stats_type in stats_type_list
                              }
        
        
# %% immediate process
class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param, del_delay):
        self.param = param
        self.del_delay = del_delay
        
        factors_related = self.param['factors_related']
        final_factors = factors_related['final']
        self.volume_type_list = final_factors['volume_type']
        self.size_div_type_list = final_factors['size_div_type']
        self.size_type_list = final_factors['size_type']
    
    def _init_container(self):
        self.a = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self.net_a = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self.factor = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
        self.update_time = {}
        self.curr_five_end = {}
        self.close = defaultdict(lambda: defaultdict(float))
        self.rtn = defaultdict(lambda: defaultdict(float))
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCSizeBar'] = self._process_cc_size_bar_msg
        self.topic_func_mapping['CCBar'] = self._process_cc_bar_msg
        
    def _process_cc_size_bar_msg(self, pb_msg):
        sbp = SizeBarProcessor(pb_msg, log=self.log)
        symbol = sbp.symbol
        ts = sbp.timestamp
        ts_in_dt = pd.to_datetime(ts, unit='ms')
        
        # 将时间戳向上取整到最近的5分钟
        nearest_five_min = self._round_to_nxt_nearest_five(ts_in_dt)
        
        for volume_type in self.volume_type_list:
            for size_div_type in self.size_div_type_list:
                for size_type in self.size_type_list:
                    self.a[nearest_five_min][(volume_type, size_div_type, size_type)][symbol] += sbp.get(
                        'A', volume_type, size=size_type, size_div=size_div_type)
                    self.net_a[nearest_five_min][(volume_type, size_div_type, size_type)][symbol] += sbp.get(
                        'NetA', volume_type, size=size_type, size_div=size_div_type)
                    
        if symbol not in self.curr_five_end:
            self.curr_five_end[symbol] = nearest_five_min
        if ts_in_dt >= self.curr_five_end[symbol]:
            curr_five_end = self.curr_five_end[symbol]
            for name in self.a[curr_five_end]:
                a_sum = self.a[curr_five_end][name][symbol]
                net_a_sum = self.net_a[curr_five_end][name][symbol]
                self.factor[curr_five_end][name][symbol] = net_a_sum / a_sum if a_sum != 0 else np.nan
            self.curr_five_end[symbol] = nearest_five_min
                    
        self.update_time[symbol] = ts
        
    def _round_to_pre_nearest_five(self, ts_in_dt):
        # 将时间戳向上取到最近的五分钟
        pre_nearest_five = (ts_in_dt - timedelta(minutes=0.1)).floor('5min')
        return pre_nearest_five
        
    def _round_to_nxt_nearest_five(self, ts_in_dt):
        # 将时间戳向上取到最近的五分钟
        nxt_nearest_five = (ts_in_dt + timedelta(minutes=4.9)).floor('5min')
        # print(ts_in_dt, nxt_nearest_five)
        return nxt_nearest_five

    def _process_cc_bar_msg(self, pb_msg):
        bp = BarProcessor(pb_msg, log=self.log)
        type_ = bp.type
        if type_ != '1m':
            return
        symbol = bp.symbol
        ts = bp.ts
        ts_in_dt = pd.to_datetime(ts, unit='ms')
        
        pre_nearest_five_min = self._round_to_pre_nearest_five(ts_in_dt)
        nxt_nearest_five_min = self._round_to_nxt_nearest_five(ts_in_dt)
        # print(symbol, ts, ts_in_dt, pre_nearest_five_min, nxt_nearest_five_min)
        if ts_in_dt == nxt_nearest_five_min:
            self.close[nxt_nearest_five_min][symbol] = bp.close
            curr_close = self.close[nxt_nearest_five_min][symbol]
            pre_close = self.close[pre_nearest_five_min][symbol]
            self.rtn[nxt_nearest_five_min][symbol] = (
                (curr_close / pre_close - 1)
                if pre_close != 0 else np.nan)
            # print(symbol, ts_in_dt, curr_close, pre_close)
            
    def delete_once(self, ts):
        del_thres = ts - self.del_delay
        containers = [self.factor, self.a, self.net_a, self.close, self.rtn]
        if_check_empty = [True, False, False, False, True]
        for i_c, container in enumerate(containers):
            for ts, ts_iv in list(container.items()):
                if ts < del_thres:
                    if if_check_empty[i_c]:
                        try:
                            assert is_empty_dict(container[ts])
                        except:
                            self.log.error('Cache to clear is not empty: {container[ts]}')
                    del container[ts]


# %%
class F04(FactorUpdaterTsFeatureOfSnaps):
    
    name = 'f04_ts_net_size_pct_of_size'
    
    def __init__(self):
        super().__init__()
    
    def _init_param_names(self):
        for pr in self.param_set:
            vt = pr['volume_type']
            sdt = pr['size_div_type']
            st = pr['size_type']
            reg_type = pr['reg_type']
            lookback_wd = pr['lookback_wd']
            mmt_wd = pr['mmt_wd']
            stats_type = pr['stats_type']
            diff_wd = pr['diff_wd']
            diff_type = pr['diff_type']
            suffix_mmt = '' if mmt_wd == '0min' else f'_mmt{mmt_wd}_{stats_type}'
            suffix_diff = '' if diff_wd == '0min' else f'_{diff_type}{diff_wd}'
            pr['name'] = f'{vt}_{sdt}_{st}_lb{lookback_wd}_{reg_type}{suffix_mmt}{suffix_diff}'
            
            pr['valid'] = True
            if mmt_wd == '0min' and stats_type != 'int':
                pr['valid'] = False
            if diff_wd == '0min' and diff_type != 'ma':
                pr['valid'] = False
                  
    def _init_lookback_mapping(self):
        raw_cache_period = self.params['record']['raw_cache_period']
        reg_cache_period = self.params['record']['reg_cache_period']
        reg_stats_cache_period = self.params['record']['reg_stats_cache_period']
        del_delay = self.params['record']['del_delay']
        lookback_wd_list = self.params['factors_related']['final']['lookback_wd']
        mmt_wd_list = self.params['factors_related']['final']['mmt_wd']
        diff_wd_list = self.params['factors_related']['final']['diff_wd']
        
        self.raw_cache_lookback = timedelta(seconds=parse_time_string(raw_cache_period))
        self.reg_cache_lookback = timedelta(seconds=parse_time_string(reg_cache_period))
        self.reg_stats_cache_lookback = timedelta(seconds=parse_time_string(reg_stats_cache_period))
        self.del_delay = timedelta(seconds=parse_time_string(del_delay))
        self.lookback_wd_mapping = {lookback_wd: timedelta(seconds=parse_time_string(lookback_wd))
                                    for lookback_wd in lookback_wd_list}
        self.mmt_wd_mapping = {mmt_wd: timedelta(seconds=parse_time_string(mmt_wd)) 
                               for mmt_wd in mmt_wd_list}
        self.diff_wd_mapping = {diff_wd: timedelta(seconds=parse_time_string(diff_wd)) 
                               for diff_wd in diff_wd_list}
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params, self.del_delay)
        # 定时记录
        self.raw_mgr = RawMgr(self.params, self.param_set, self.cache_dir, 
                              self.raw_cache_lookback, file_name='raw', log=self.log)
        self.reg_mgr = RegMgr(self.params, self.param_set, self.cache_dir, 
                                self.reg_cache_lookback, file_name='reg', log=self.log)
        self.reg_stats_mgr = RegStatsMgr(self.params, self.param_set, self.cache_dir, 
                                         self.reg_stats_cache_lookback, file_name='reg_stats', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)
        
    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Calc Reg", 'minute', 30, 
                                             self._calc_reg)
        self.task_scheduler['calc'].add_task("30 Minutes Calc Reg Stats", 'minute', 30, 
                                             self._calc_reg_stats)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)

    @timeit
    def _iv_record(self, ts):
        self._record_size_bar_info()
        self._record_rtn_info()
        self.immediate_mgr.delete_once(ts)

    def _record_size_bar_info(self):
        for ts_of_data, iv_of_ts in list(self.immediate_mgr.factor.items()):
            iv_of_ts = iv_of_ts.copy()
            for name, ts_name_iv in list(iv_of_ts.items()):
                ts_name_iv_cut = ts_name_iv.copy()
                if not ts_name_iv_cut:
                    continue
                self.raw_mgr.add_row(name, ts_name_iv_cut, ts_of_data)
                # print(name, self.raw_mgr[name])
                for symbol in ts_name_iv_cut:
                    del self.immediate_mgr.factor[ts_of_data][name][symbol]

    def _record_rtn_info(self):
        for ts_of_data, ts_iv in list(self.immediate_mgr.rtn.items()):
            ts_iv_cut = ts_iv.copy()
            self.raw_mgr.add_row('rtn', ts_iv_cut, ts_of_data)
            for symbol in ts_iv_cut:
                del self.immediate_mgr.rtn[ts_of_data][symbol]
        # print('rtn', self.raw_mgr['rtn'])
                    
    @timeit
    def _calc_reg(self, ts):
        final_factors = self.params['factors_related']['final']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        reg_type_list = final_factors['reg_type']
        lookback_wd_list = final_factors['lookback_wd']
        rtn = self.raw_mgr['rtn'].copy()
        if len(rtn) < 3:
            return
        idx_to_update = rtn.index[-3:]
        for volume_type in volume_type_list:
            for size_div_type in size_div_type_list:
                for size_type in size_type_list:
                    for lookback_wd in lookback_wd_list:
                        lookback_wd_timedelta = self.lookback_wd_mapping[lookback_wd]
                        var = self.raw_mgr[(volume_type, size_div_type, size_type)]
                        for ts_of_data in idx_to_update:
                            lookback_rtn = rtn.loc[ts-lookback_wd_timedelta:]
                            lookback_var = var.loc[ts-lookback_wd_timedelta:]
                            reg_res = safe_ts_regress_once(lookback_rtn, lookback_var)
                            if not reg_res:
                                continue
                            for reg_type in reg_type_list:
                                self.reg_mgr.add_row((volume_type, size_div_type, size_type, 
                                                      reg_type, lookback_wd), 
                                                     reg_res[reg_type], ts_of_data)
                                # print((volume_type, size_div_type, size_type, 
                                #                       reg_type, lookback_wd),
                                #       self.reg_mgr[(volume_type, size_div_type, size_type, 
                                #                       reg_type, lookback_wd)])
                                
    @timeit
    def _calc_reg_stats(self, ts):
        final_factors = self.params['factors_related']['final']
        volume_type_list = final_factors['volume_type']
        size_div_type_list = final_factors['size_div_type']
        size_type_list = final_factors['size_type']
        reg_type_list = final_factors['reg_type']
        lookback_wd_list = final_factors['lookback_wd']
        stats_type_list = final_factors['stats_type']
        mmt_wd_list = final_factors['mmt_wd']

        for volume_type in volume_type_list:
            for size_div_type in size_div_type_list:
                for size_type in size_type_list:
                    for lookback_wd in lookback_wd_list:
                            for reg_type in reg_type_list:
                                factor_org = self.reg_mgr[(volume_type, size_div_type, size_type, 
                                                           reg_type, lookback_wd)]
                                if len(factor_org) == 0:
                                    continue
                                for mmt_wd in mmt_wd_list:
                                    mmt_wd_lookback = self.mmt_wd_mapping[mmt_wd]
                                    for stats_type in stats_type_list:
                                        factor_stats = ts_basic_stat(factor_org, ts, mmt_wd_lookback, 
                                                                     stats_type=stats_type)
                                        self.reg_stats_mgr.add_row((volume_type, size_div_type, size_type, 
                                                                    reg_type, lookback_wd, mmt_wd, stats_type), 
                                                                   factor_stats, ts)
                                        # print((volume_type, size_div_type, size_type, 
                                        #                             reg_type, lookback_wd, mmt_wd, stats_type), 
                                        #       self.reg_stats_mgr[(volume_type, size_div_type, size_type, 
                                        #                             reg_type, lookback_wd, mmt_wd, stats_type)])

    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            if not pr['valid']:
                continue
            vt = pr['volume_type']
            sdt = pr['size_div_type']
            st = pr['size_type']
            reg_type = pr['reg_type']
            lookback_wd = pr['lookback_wd']
            mmt_wd = pr['mmt_wd']
            stats_type = pr['stats_type']
            diff_wd = pr['diff_wd']
            diff_type = pr['diff_type']
            pr_name = pr['name']
            factor_org = self.reg_stats_mgr[(vt, sdt, st, 
                                             reg_type, lookback_wd, mmt_wd, stats_type)]
            if len(factor_org) == 0:
                continue
            diff_wd_lookback = self.diff_wd_mapping[diff_wd]
            if diff_wd == '0min':
                factor_final = factor_org.iloc[-1]
            elif diff_type == 'ma':
                factor_final = factor_org.loc[ts-diff_wd_lookback:].mean(axis=0)
            elif diff_type == 'pchg':
                factor_ma = factor_org.loc[ts-diff_wd_lookback:].mean(axis=0)
                factor_final = factor_org / factor_ma - 1
            self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final)
            temp_dict[pr_name] = factor_final
            # print(pr_name, factor_final)
        return temp_dict
    
    @timeit
    def _save_to_cache(self, ts):
        self.raw_mgr.save(ts)
        self.reg_mgr.save(ts)
        self.reg_stats_mgr.save(ts)

        
# %%
if __name__=='__main__':
    updater = F04()
    updater.run()
        