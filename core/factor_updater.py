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
from pathlib import Path
import toml
import itertools
import time
import os
from abc import ABC, abstractmethod
import signal
from datetime import timedelta
from pympler.asizeof import asizeof
import tracemalloc
import threading
import warnings
warnings.simplefilter("ignore")


from receiver.rcv_fr_lord import LordWithFilter
from core.database_handler import DatabaseHandler
from utils.logutils import FishStyleLogger
from utils.dirutils import load_path_config
from utils.market import load_binance_data, get_binance_tick_size, usd
from utils.decorator_utils import timeit
from utils.timeutils import parse_time_string
from core.task_scheduler import TaskScheduler
from core.cache_persist_manager import saving_event


# %%
class FactorUpdater(ABC):
    
    def __init__(self):
        self._load_path_config()
        self._init_dir()
        self._load_param()
        self._load_param_info()
        self._load_exchange_info_detail()
        self._init_log()
        self._init_msg_controller()
        self._init_database_handler()
        self._init_param_set()
        self._set_up_signal_handler()
        self.saving_event = saving_event
        self.running = True
        
    def _load_path_config(self):
        file_path = Path(__file__).resolve()
        project_dir = file_path.parents[1]
        self.path_config = load_path_config(project_dir)
        
    def _init_dir(self):
        path_config = self.path_config
        
        self.param_dir = Path(path_config['param'])
        cache_dir = Path(path_config['cache'])
        persist_dir = Path(path_config['persist'])
        self.cache_dir = cache_dir / self.name 
        self.persist_dir = persist_dir / self.name
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_param(self):
        self.params = toml.load(self.param_dir / f'{self.name}.toml')
        
    def _load_param_info(self):
        self.address = self.params['address']
        self.topic_list = self.params['topic']
        self.mysql_name = self.params['mysql_name']
        self.author = self.params['author']
        self.category = self.params['category']
        
    def _init_param_set(self):
        factor_related = self.params.get('factors_related', {})
        if factor_related:
            final_param = factor_related.get('final')
            self.param_set = para_allocation(final_param) if final_param else {}
            
    def _load_exchange_info_detail(self):
        self.exchange_info_dir = Path(self.path_config['exchange_info'])
        exchange = self.params['exchange']
        self.exchange = globals()[exchange]

    def _init_log(self):
        self.log = FishStyleLogger()
        
    def _init_msg_controller(self):
        self.msg_controller = LordWithFilter(self.topic_list, address=self.address, log=self.log)
        
    def _init_database_handler(self):
        self.db_handler = DatabaseHandler(self.mysql_name, log=self.log)
        
    def _set_up_signal_handler(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGSEGV, self._signal_handler)
        signal.signal(signal.SIGILL, self._signal_handler)
        
    def _signal_handler(self, sig, frame):
        self.log.warning("收到终止信号，正在清理资源...")
        self.stop()
        if self.saving_event.is_set():
            self.log.warning("检测到保存操作正在进行，等待保存完成...")
            self.saving_event.wait()  # 等待保存完成
        else:
            self.log.info("没有保存操作正在进行")
        time.sleep(15)
        self.log.info("清理完成，程序安全退出")
        os._exit(0)
        
    @abstractmethod
    def run(self):
        pass
    
    @abstractmethod
    def stop(self):
        pass
    
    def raise_signal(self):
        signal.raise_signal(signal.SIGTERM)
    

# %%
class FactorUpdaterWithTickSize(FactorUpdater):
    
    def __init__(self):
        super().__init__()
        self.reload_tick_size_mapping(0)

    def reload_tick_size_mapping(self, ts):
        exchange_info = load_binance_data(self.exchange, self.exchange_info_dir)
        self.tick_size_mapping = get_binance_tick_size(exchange_info)
            
        
class FactorUpdaterTsFeatureOfSnaps(FactorUpdater):
    
    def __init__(self):
        super().__init__()
        
        self._init_param_names()
        self._init_lookback_mapping()
        self._init_task_scheduler()
        self._init_managers()
        self.reload_exchange_info(0)
        self._add_tasks()
        tracemalloc.start()
        
    def reload_exchange_info(self, ts):
        exchange_info = load_binance_data(self.exchange, self.exchange_info_dir)
        trading_symbols = [symbol_info['symbol'].lower() for symbol_info in exchange_info['symbols']
                           if (
                                   symbol_info['status'] == 'TRADING' 
                                   and symbol_info['symbol'].endswith('USDT')
                               )]
        self.trading_symbols = sorted(trading_symbols)
        self._notify_exchange_info(self.trading_symbols)
        
    def _notify_exchange_info(self, trading_symbols):
        self.immediate_mgr.reset_trading_symbols(trading_symbols)
    
    @abstractmethod
    def _init_param_names(self):
        pass
        
    def _init_lookback_mapping(self):
        cache_period = self.params['record']['cache_period']
        mmt_wd_list = self.params['factors_related']['final']['mmt_wd']
        
        self.cache_lookback = timedelta(seconds=parse_time_string(cache_period))
        self.mmt_wd_lookback_mapping = {mmt_wd: timedelta(seconds=parse_time_string(mmt_wd)) 
                                        for mmt_wd in mmt_wd_list}
    
    @abstractmethod
    def _init_managers(self):
        pass
    
    def _init_task_scheduler(self):
        self.task_scheduler = {name: TaskScheduler(log=self.log) for name in ['calc', 'io']}
        
    def _add_tasks(self): # default
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Get Snapshot", 'minute', 1, self._get_snapshot)
        self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        self.task_scheduler['calc'].add_task("1 Minute Log Queue Size", 'minute', 1, self._log_queue_size)
        self.task_scheduler['calc'].add_task("Reload Exchange Info", 'specific_time', ['00:05'], 
                                     self.reload_exchange_info)
        # self.task_scheduler['calc'].add_task("1 Minute Monitor", 'minute', 1, self._monitor_usage)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)
        
    @timeit
    def _get_snapshot(self, ts):
        self._wait_for_complete_ts_data(ts)
        min_lob = self.immediate_mgr.get_minute_lob(ts)
        self.immediate_mgr.get_one_snapshot(ts, min_lob)
        self.immediate_mgr.clear_container_before_ts(ts)
    
    @timeit
    def _wait_for_complete_ts_data(self, ts):
        while True:
            if self.immediate_mgr.newest_ts <= ts:
                time.sleep(1)
            else:
                break
            
    @timeit
    def _iv_record(self, ts):
        for name, iv in list(self.immediate_mgr.factor.items()):
            self.cache_mgr.add_row(name, iv, ts)
    
    def _final_calc_n_send_n_record(self, ts):
        temp_dict = self._final_calc(ts)
        self._final_send(ts, temp_dict)
        self._final_record(ts, temp_dict)
        
    @abstractmethod
    def _final_calc(self, ts):
        pass
    
    @timeit
    def _final_send(self, ts, temp_dict):
        with self.db_handler as db:
            for pr_name, factor_info in temp_dict.items():
                data_to_write = {
                    'author': self.author,
                    'category': self.category,
                    'pr_name': pr_name, 
                    'factor_final': None,
                    'ts': ts,
                    }
                if isinstance(factor_info, dict):
                    for k, v in factor_info.items():
                        data_to_write[k] = v
                else:
                    data_to_write['factor_final'] = factor_info
                db.batch_insert_data(*data_to_write.values())
    
    @timeit
    def _final_record(self, ts, temp_dict):
        for pr_name, factor_info in temp_dict.items():
            factor_new_row = factor_info['factor_final'] if isinstance(factor_info, dict) else factor_info
            self.persist_mgr.add_row(pr_name, factor_new_row, ts)
            
        self.persist_mgr.add_row('update_time', self.immediate_mgr.update_time, ts)
        
    def _log_queue_size(self, ts):
        self.immediate_mgr.log_queue_size()
        
    def _monitor_usage(self, ts):
        self.log.info(f'cache.container: {convert_size(asizeof(self.cache_mgr.cache))}')
        self.log.info(f'persist.container: {convert_size(asizeof(self.persist_mgr.factor_persist))}')
        self.log.info(f'queue: {convert_size(asizeof(self.msg_controller._queue_map))}') # 没用，不会读到proto的对象
        
# =============================================================================
#         self.log.info(f'msg: {convert_size(asizeof(self.msg_controller))}')
#         self.log.info(f'immediate: {convert_size(asizeof(self.immediate_mgr))}')
#         self.log.info(f'task: {convert_size(asizeof(self.task_scheduler))}')
#         self.log.info(f'cache: {convert_size(asizeof(self.cache_mgr))}')
#         self.log.info(f'persist: {convert_size(asizeof(self.persist_mgr))}')
#         self.log.info(f'db: {convert_size(asizeof(self.db_handler))}')
# =============================================================================

        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')
        
        print("[ Top 10 memory consuming lines ]")
        for stat in top_stats[:10]:
            print(stat)

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
            
            
class FactorUpdaterTsFeatureOfSnapsWithTickSize(FactorUpdaterTsFeatureOfSnaps,
                                                FactorUpdaterWithTickSize):
    
    def __init__(self):
        super().__init__()
    
    def _add_tasks(self): # default
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Get Snapshot", 'minute', 1, self._get_snapshot)
        self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        self.task_scheduler['calc'].add_task("1 Minute Log Queue Size", 'minute', 1, self._log_queue_size)
        self.task_scheduler['calc'].add_task("Reload Exchange Info", 'specific_time', ['00:05'], 
                                     self.reload_exchange_info)
        self.task_scheduler['calc'].add_task("Reload Tick Size Mapping", 'specific_time', ['00:05'], 
                                     self.reload_tick_size_mapping)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)
        
# =============================================================================
#         ## calc
#         self.task_scheduler['calc'].add_task("1 Minute Get Snapshot", 'second', 5, self._get_snapshot)
#         self.task_scheduler['calc'].add_task("1 Minute Record", 'second', 5, self._iv_record)
#         self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
#                                              self._final_calc_n_send_n_record)
#         self.task_scheduler['calc'].add_task("1 Minute Log Queue Size", 'minute', 1, self._log_queue_size)
#         self.task_scheduler['calc'].add_task("Reload Tick Size Mapping", 'specific_time', ['00:05'], 
#                                      self.reload_tick_size_mapping)
#         
#         ## io
#         self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
#         self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)
# =============================================================================
    

class FactorUpdaterTsFeatureOfSnapsTest(FactorUpdaterTsFeatureOfSnaps):
    
    def _final_calc_n_send_n_record(self, ts):
        return
        temp_dict = self._final_calc(ts)
        self._final_send(ts, temp_dict)
        self._final_record(ts, temp_dict)
        
    def _add_tasks(self): # default
        # 按同时触发时预期的执行顺序排列
        # 本部分需要集成各类参数与mgr，故暂不做抽象
        # 此处时间参数应为1min和30min，为了测试更快看到结果，暂改为1min -> 3s，30min -> 1min
        
        ## calc
        self.task_scheduler['calc'].add_task("1 Minute Get Snapshot", 'minute', 1, self._get_snapshot)
        self.task_scheduler['calc'].add_task("1 Minute Record", 'minute', 1, self._iv_record)
        self.task_scheduler['calc'].add_task("30 Minutes Final and Send", 'minute', 30, 
                                             self._final_calc_n_send_n_record)
        self.task_scheduler['calc'].add_task("1 Minute Log Queue Size", 'minute', 1, self._log_queue_size)
        self.task_scheduler['calc'].add_task("Reload Exchange Info", 'specific_time', ['00:05'], 
                                     self.reload_exchange_info)
        # self.task_scheduler['calc'].add_task("1 Minute Monitor", 'minute', 1, self._monitor_usage)
        
        ## io
        self.task_scheduler['io'].add_task("5 Minutes Save to Cache", 'minute', 5, self._save_to_cache)
        self.task_scheduler['io'].add_task("30 Minutes Save to Persist", 'minute', 30, self._save_to_final)
        
        
    def run(self):
        self.msg_controller.start() # 占一条线程，用于收取lord消息存队列
        self.immediate_mgr.start() # 占一条线程，用于即时处理队列消息
        # self.task_scheduler['io'].start() # 占2跳线程：处理任务 + 任务调度
        self.task_scheduler['calc'].start(use_thread_for_task_runner=False) # 主线程：处理任务 + 辅助线程：任务调度
        
        
# %%
def convert_size(size_bytes):
    """
    将字节数转换为可读的 KB, MB, GB 等形式
    """
    if size_bytes == 0:
        return "0B"
    size_name = ("B", "KB", "MB", "GB", "TB", "PB")
    i = 0  # 指定单位索引
    while size_bytes >= 1024 and i < len(size_name) - 1:
        size_bytes /= 1024.0
        i += 1
    return f"{size_bytes:.2f} {size_name[i]}"
        

# %%
def para_allocation(para_dict):
    # 参数排列组合
    allocated_para_detail = list(itertools.product(*para_dict.values()))
    # 参数组匹配参数名
    allocated_para_detail_with_name = list(
        map(lambda x: dict(zip(para_dict.keys(), list(x))), allocated_para_detail))
    return allocated_para_detail_with_name

        