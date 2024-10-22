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
from datetime import timedelta, datetime
import pandas as pd


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnaps
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import ImmediateProcessManager, BarProcessor
from utils.decorator_utils import timeit
from utils.timeutils import parse_time_string
from utils.datautils import is_empty_dict
from utils.calc import get_last_valid_values_from_index


# %% cache & persist
class RawMgr(CacheManager):
    
    def init_cache_mapping(self):
        self.cache_mapping.update({'close': 'close'})


# %% immediate process
def convert_to_previous_3s(timestamp: datetime) -> datetime: # !!!: temp
    seconds = timestamp.second
    remainder = seconds % 3
    new_seconds = seconds - remainder
    return timestamp.replace(second=new_seconds, microsecond=0)


class MyImmediateProcessMgr(ImmediateProcessManager):

    def load_info(self, param, del_delay):
        self.param = param
        self.del_delay = del_delay
    
    def _init_container(self):
        self.close = defaultdict(lambda: defaultdict(float))
        self.update_time = {}
    
    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCBar'] = self._process_cc_bar_msg

    def _process_cc_bar_msg(self, pb_msg):
        bp = BarProcessor(pb_msg)
        type_ = bp.type
        if type_ != '3s':
            return
        symbol = bp.symbol
        ts = bp.ts
        ts_in_dt = pd.to_datetime(ts, unit='ms')
        # ts_in_dt = convert_to_previous_3s(ts_in_dt)  # !!!: temp
        
        now = datetime.utcnow()
        diff = now - ts_in_dt
        if diff > timedelta(minutes=1):
            print(symbol, diff, now, ts_in_dt)
        
        self.close[ts_in_dt][symbol] = bp.close
        self.update_time[symbol] = ts
            
    def delete_once(self, ts):
        del_thres = ts - self.del_delay
        containers = [self.close]
        if_check_empty = [True]
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
class FactorsForPMV0(FactorUpdaterTsFeatureOfSnaps):
    
    name = 'factors_for_portfolio_management_v0'
    
    def __init__(self):
        super().__init__()
        
        self.pre_ts = None
    
    def _init_param_names(self):
        for pr in self.param_set:
            twap_wd = pr['twap_wd']
            pr['name'] = 'curr_price' if twap_wd == '0min' else f'twap_{twap_wd}'
                  
    def _init_lookback_mapping(self):
        raw_cache_period = self.params['record']['raw_cache_period']
        del_delay = self.params['record']['del_delay']
        curr_max_lookback = self.params['record']['curr_max_lookback']
        twap_wd_list = self.params['factors_related']['final']['twap_wd']
        
        self.raw_cache_lookback = timedelta(seconds=parse_time_string(raw_cache_period))
        self.del_delay = timedelta(seconds=parse_time_string(del_delay))
        self.curr_max_lookback = timedelta(seconds=parse_time_string(curr_max_lookback))
        self.twap_wd_mapping = {twap_wd: timedelta(seconds=parse_time_string(twap_wd)) 
                               for twap_wd in twap_wd_list}
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params, self.del_delay)
        # 定时记录
        self.raw_mgr = RawMgr(self.params, self.param_set, self.cache_dir, 
                              self.raw_cache_lookback, file_name='raw', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)
        
    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("3 Second Record", 'second', 3, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        self.task_scheduler['calc'].add_task("30 Minutes Update Pre TS", 'minute', 30, 
                                             self._update_pre_ts)
        self.task_scheduler['calc'].add_task("1 Minutes Del Immediate Record", 'minute', 1, 
                                             self._delete_once)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)

    @timeit
    def _iv_record(self, ts):
        self._record_close_info()
        
    def _record_close_info(self):
        for ts_of_data, ts_iv in list(self.immediate_mgr.close.items()):
            ts_iv_cut = ts_iv.copy()
            self.raw_mgr.add_row('close', ts_iv_cut, ts_of_data)
            for symbol in ts_iv_cut:
                del self.immediate_mgr.close[ts_of_data][symbol]
    
    @timeit
    def _delete_once(self, ts):
        self.immediate_mgr.delete_once(ts)
                    
    @timeit
    def _final_calc_n_send(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            twap_wd = pr['twap_wd']
            pr_name = pr['name']
            close_org = self.raw_mgr['close']
            if len(close_org) == 0:
                continue
            
            factor_final = None
            ts_to_record = None
            if twap_wd == '0min':
                factor_final = get_last_valid_values_from_index(close_org, ts-self.curr_max_lookback)
                ts_to_record = ts
            else:
                twap_wd_real = self.twap_wd_mapping[twap_wd]
                if self.pre_ts is not None and self.pre_ts + twap_wd_real <= ts: # TODO: 当前仅支持twap时间小于计算时间间隔，若有进一步需求再拓展
                    factor_final = close_org.loc[self.pre_ts:(self.pre_ts+twap_wd_real)].mean(axis=0)
                    ts_to_record = self.pre_ts
                    
            if factor_final is not None:
                self.db_handler.batch_insert_data(self.author, self.category, pr_name, factor_final, ts_to_record)
                temp_dict[pr_name] = (ts_to_record, factor_final)
        return temp_dict
    
    @timeit
    def _final_record(self, ts, temp_dict):
        for pr_name, factor_new_row_info in temp_dict.items():
            ts_to_record, factor_new_row = factor_new_row_info
            self.persist_mgr.add_row(pr_name, factor_new_row, ts_to_record)
            
        self.persist_mgr.add_row('update_time', self.immediate_mgr.update_time, ts)
        
    def _update_pre_ts(self, ts):
        self.pre_ts = ts
    
    @timeit
    def _save_to_cache(self, ts):
        self.raw_mgr.save(ts)

        
# %%
if __name__=='__main__':
    updater = FactorsForPMV0()
    updater.run()
        