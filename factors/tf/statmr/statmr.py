
"""

Author: TIAN Fang
Date: Sep 12, 2024

"""

# %% imports
import os
import sys
import json
import warnings
warnings.filterwarnings("ignore")
import itertools
import numpy as np
import pandas as pd

from pathlib import Path
from copy import deepcopy
from time import sleep
from datetime import timedelta
from collections import defaultdict

file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[3]
sys.path.append(str(project_dir))

from core.factor_updater import FactorUpdater
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import CacheManager, PersistenceManager
from core.immediate_process_manager import ImmediateProcessManager
from utils.datautils import add_row_to_dataframe_reindex
from utils.data_parser import convert_to_lowercase
from utils.timeutils import parse_time_string
from utils.functions import ts_basic_stat


# % % cache & persist
class MyCacheMgr(CacheManager):
    def init_cache_mapping(self):
        self.cache_mapping = {cvn: cvn for cvn in self.params['cache_varnames']}
            
            
class MyPersistenceMgr(PersistenceManager):
    def init_persist_list(self):
        self.persist_list = []
        
        
# % % immediate process
class MyImmediateProcessMgr(ImmediateProcessManager):
    
    # 此处未定义__init__，实际使用时，若即时计算需要用到某些参数，可以设置__init__传入
    def _init_container(self):
        self.data = defaultdict(list) # store fetched data
        self.trade = defaultdict(list) # 存储每个symbol对应的逐笔trade数据
        self.update_time = {} # symbol最后的更新时间
    
    def _init_topic_func_mapping(self):
        # self.topic_func_mapping['CCBar'] = self._process_cc_bar
        self.topic_func_mapping['CCTrade'] = self._process_cc_trade
        
    def _process_cc_bar(self, pb_msg):
        header = pb_msg.header
        symbol = convert_to_lowercase(header.symbol)
        ts = header.timestamp
        
        # print(ts, symbol, pb_msg.bar.volume, pb_msg.bar.turnover)
        self.update_time[symbol] = ts
    
    def _process_cc_trade(self, pb_msg):
        header = pb_msg.header
        symbol = convert_to_lowercase(header.symbol)
        ts = header.timestamp
        
        # print(symbol, ts, pb_msg.trade)
        self.data['trade'].append((symbol, ts, pb_msg.trade))
        self.update_time[symbol] = ts

# %%
class Statmr(FactorUpdater):
    
    name = 'statmr'
    
    def __init__(self):
        super().__init__()
        
        self._init_params()
        self._init_lookback_mapping()
        self._init_managers()
        
        self._load_size_threshold()
        # self._load_init_cahe()
        self._load_init_persist()
        
        self._init_task_scheduler()
        self._add_tasks()
        
        self._load_factor_params()
    
    def _load_factor_params(self):
        factor_syn_info_path = file_dir / 'syn_info_dict_statmr.json'
        factor_syn_info_dict = json.load(open(factor_syn_info_path, 'r'))
        self.factor_info_list = list(factor_syn_info_dict.values())
        
        
    def _init_params(self):
        
        for pr in self.param_set:
            amount_type = pr['amount_type']
        
        self.min_types = self.params.get('min_types', ['1min', '5min'])
        self.size_types = self.params.get('size_types', list('SMLX')) # must be monotonic
        self.volume_types = self.params.get('volume_types', ['amount', 'volume', 'tradenum'])
        self.sides = self.params.get('sides', ['B', 'S'])
        self.side_size_labels = [f'{side}_Quantile_{st}' for side in self.sides for st in self.size_types]
        self.task_names = self.params.get('task_names', ['calc', 'io'])
        
        self.cache_filename = self.params.get('cache_filename', 'trade_size')
        self.params['cache_varnames'] = [f'{vt}_{sst}' for vt in self.volume_types for sst in self.side_size_labels]
        # self.cache_mgr.cache = defaultdict(pd.DataFrame)
        
        self.persist_vars = defaultdict(pd.DataFrame)
        self.persist_varnames = [f'{vt}_{st}_bsimb' for vt in self.volume_types for st in self.size_types]
        
    def _init_lookback_mapping(self):
        cache_period = self.params['record']['cache_period']
        self.cache_lookback = timedelta(seconds=parse_time_string(cache_period))
    
    def _load_size_threshold(self):
        """ load trade and order size thresholds """
        trade_size_threshold = pd.read_parquet(os.path.join(self.path_config['size_params'], 'trade_size_threshold.parquet'))
        self.trade_size_threshold = trade_size_threshold.T.to_dict('list')
        del trade_size_threshold
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=None)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, self.cache_lookback,
                                    file_name=self.cache_filename, log=self.log)
        self.persist_mgr = MyPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)
    
    def _load_init_cahe(self):
        for varname in self.params['cache_varnames']:
            try:
                self.cache_mgr.cache[varname] = pd.read_parquet(self.cache_dir / f'{varname}.parquet')
            except Exception:
                self.cache_mgr.cache[varname] = pd.DataFrame()
    
    def _load_init_persist(self):
        for fn in self.persist_varnames:
            try:
                self.persist_vars[fn] = pd.read_parquet(self.persist_dir / f'{fn}.parquet')
            except Exception:
                self.persist_vars[fn] = pd.DataFrame()
    
    # @run_by_thread
    def _save_cache(self, max_rowno: int):
        dc_cache_vars = deepcopy(self.cache_mgr.cache)
        for varname, df in dc_cache_vars.items():
            df.iloc[-max_rowno:].to_parquet(self.cache_dir / f'{varname}.parquet')
    
    # @run_by_thread
    def _save_persist(self):
        dc_persist_vars = deepcopy(self.persist_vars)
        for varname, df in dc_persist_vars.items():
            df.to_parquet(self.persist_dir / f'{varname}.parquet')
            
    
    def _init_task_scheduler(self):
        self.task_scheduler = {name: TaskScheduler(log=self.log) for name in self.task_names}
    
    def _add_tasks(self):
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 60, self._minute_record)
        self.task_scheduler['io'].add_task("5 Minutes Calculate Factor & Save to Persist", 'minute', 2, self._minute_save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Calculate Factor & Save to Persist", 'minute', 3, self._half_hour_save_factor)
    
    def _minute_record(self, ts):
        """record size volumes every 60 seconds"""
        print(f'[minute_record]: scheduled {ts}, real {pd.Timestamp.now(tz="utc")}')
        for data_type in sorted(self.immediate_mgr.data.keys()):
            
            print(data_type, len(self.immediate_mgr.data[data_type]))
            data = self.immediate_mgr.data[data_type]
            fetch_length = len(data) # 此时切出来的数据大小
            
            # 清空fetch过的数据
            print(data_type, fetch_length, len(data), len(self.immediate_mgr.data[data_type]))
            self.immediate_mgr.data[data_type] = self.immediate_mgr.data[data_type][fetch_length:]
            print(data_type, fetch_length, len(data), len(self.immediate_mgr.data[data_type]))
            
            # 逐symbol计算大小单
            cache_trade = defaultdict(list)
            for symbol, _, batch_trade in data:
                if len(batch_trade) > 0:
                    for trade in batch_trade:
                        cache_trade[symbol].append((trade.timestamp, trade.side, trade.volume, trade.amount))
            
            amount, volume, tradenum = defaultdict(list), defaultdict(list), defaultdict(list)
            for symbol in sorted(cache_trade.keys()):
                total_sides = defaultdict(str)
                total_amount = defaultdict(float)
                total_volume = defaultdict(float)
                for trade in cache_trade[symbol]:
                    total_sides[(trade[0], trade[1])] = trade[1]
                    total_volume[(trade[0], trade[1])] += trade[2]
                    total_amount[(trade[0], trade[1])] += trade[3]
                total_sides = np.array(list(total_sides.values()))
                total_volume = np.array(list(total_volume.values()))
                total_amount = np.array(list(total_amount.values()))
                
                # use total_amonut to distinguish large and small trades
                this_trade_size_thresh = self.trade_size_threshold.get(symbol, [100, 10000, 100000])
                categories = np.digitize(total_amount, bins=[0] + this_trade_size_thresh + [np.inf])
                
                amt, vlm, tdn = np.zeros(8), np.zeros(8), np.zeros(8)
                for sk in range(2):
                    is_this_side = total_sides == self.sides[sk]
                    for ck in range(4):
                        is_this_cat = (categories == ck + 1) & is_this_side
                        amt[ck + sk * 4] += np.sum(total_amount[is_this_cat])
                        vlm[ck + sk * 4] += np.sum(total_volume[is_this_cat])
                        tdn[ck + sk * 4] = np.sum(is_this_cat)
                amount[symbol], volume[symbol], tradenum[symbol] = amt, vlm, tdn
            amount = pd.DataFrame.from_dict(amount, orient='index', columns=self.side_size_labels)
            volume = pd.DataFrame.from_dict(volume, orient='index', columns=self.side_size_labels)
            tradenum = pd.DataFrame.from_dict(tradenum, orient='index', columns=self.side_size_labels).astype(int)
            
            # save trade_size one by one
            for vt, vl in zip(['amount', 'volume', 'tradenum'], [amount, volume, tradenum]):
                for sst in self.side_size_labels:
                    self.cache_mgr.cache[f'{vt}_{sst}'] = add_row_to_dataframe_reindex(df=self.cache_mgr.cache[f'{vt}_{sst}'],
                                                                                       new_data=vl[sst], index=ts)
            
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
    
    def _minute_save_to_cache(self, ts):
        sleep(20)
        print(f'[cache_save]: scheduled {ts}, real {pd.Timestamp.now(tz="utc")}')
        self.cache_mgr.save(ts)
        print(f'[cache_save_finish]: scheduled {ts}, real {pd.Timestamp.now(tz="utc")}')
        
    def _half_hour_save_factor(self, ts):
        sleep(30)
        
        print(f'[cal_factor]: scheduled {ts}, real {pd.Timestamp.now(tz="utc")}')
        temp_vars = deepcopy(self.cache_mgr.cache)
        
        # [全量的因子特征]
        basic_stats_dict = dict() # 储存不同频率的特征
        for vt, side, mt, st in itertools.product(self.volume_types, self.sides, self.min_types, ['S', 'X']):
            vsn = f'{vt}_{side}_Quantile_{st}'
            vs_size = temp_vars[vsn].resample(mt).sum()
            basic_stats_dict[(vt, side, mt, st)] = ts_basic_stat(df=vs_size, window=240, min_periods=5, step=1)
        
        for fac_info in self.factor_info_list[:1]:
            print(fac_info)
            vt, side, st, num_mt, den_mt, params = fac_info
            for method in sorted(params.keys()):
                denominator = basic_stats_dict[(vt, side, den_mt, st)][method]
                numerator = basic_stats_dict[(vt, side, num_mt, st)][method].reindex(index=denominator.index)
                factor = numerator / denominator
                
                factor_name = f"{side}A_{vt}_{st}_{method}_{num_mt.replace('min', '')}To{den_mt}"
                self.persist_mgr.persist_list.append(factor_name)
                self.persist_mgr.factor_persist[factor_name] = factor
        
        self.persist_mgr.save(ts) # save the calculated factors

    def run(self):
        self.msg_controller.start() # 占一条线程，用于收取lord消息存队列
        self.immediate_mgr.start() # 占一条线程，用于即时处理队列消息
        self.task_scheduler['io'].start() # 主线程：处理任务 + 辅助线程：任务调度
        self.task_scheduler['calc'].start(use_thread_for_task_runner=False) # 主线程：处理任务 + 辅助线程：任务调度
        
        
    def stop(self):
        self._running = False
        self.msg_controller.stop()
        self.immediate_mgr.stop()
        for task_name, task_scheduler in self.task_scheduler.items():
            task_scheduler.stop()
    
        
# %%
if __name__=='__main__':
    test = Statmr()
    test.run()
    
# %%
