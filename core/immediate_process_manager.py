# -*- coding: utf-8 -*-
"""
Created on Tue Sep 10 16:21:26 2024

@author: Xintang Zheng

星星: ★ ☆ ✪ ✩ 🌟 ⭐ ✨ 🌠 💫 ⭐️
勾勾叉叉: ✓ ✔ ✕ ✖ ✅ ❎
报警啦: ⚠ ⓘ ℹ ☣
箭头: ➔ ➜ ➙ ➤ ➥ ↩ ↪
emoji: 🔔 ⏳ ⏰ 🔒 🔓 🛑 🚫 ❗ ❓ ❌ ⭕ 🚀 🔥 💧 💡 🎵 🎶 🧭 📅 🤔 🧮 🔢 📊 📈 📉 🧠 📝

"""
# %% imports
from abc import ABC, abstractmethod
import numpy as np
import traceback
from functools import cached_property
from enum import Enum


from utils.decorator_utils import run_by_thread
from utils.data_parser import convert_to_lowercase
from utils.calc import if_ticktimes
from utils.market import MINIMUM_SIZE_FILTER


# %%
class ImmediateProcessManager(ABC):
    
    def __init__(self, topic_list, msg_controller, log=None):
        self.topic_list = topic_list
        self.topic_func_mapping = {}
        self.msg_controller = msg_controller
        self.log = log
        
        self._init_container()
        self._init_topic_func_mapping()
        self._running = True
        
    @abstractmethod
    def _init_container(self):
        pass
    
    @abstractmethod
    def _init_topic_func_mapping(self):
        pass
    
    @run_by_thread
    def _loop_processing(self, topic):
        while self._running:
            pb_msg = self.msg_controller[topic].get()
            self.topic_func_mapping[topic](pb_msg)
            
    def start(self):
        for topic in self.topic_list:
            self._loop_processing(topic)
            
    def stop(self):
        self._running = False
        
        
# %% Processor
class Processor:
    
    def __init__(self, pb_msg, log):
        header = pb_msg.header
        self.symbol = convert_to_lowercase(header.symbol)
        self.ts = header.timestamp
        self.log = log
        
    
# %% level
class LevelProcessor(Processor):
    
    def __init__(self, pb_msg, log=None):
        super().__init__(pb_msg, log=log)
        bid_info, ask_info = pb_msg.bid, pb_msg.ask
        self._get_info_from_level(bid_info, 'bid')
        self._get_info_from_level(ask_info, 'ask')

    def _get_info_from_level(self, side_info, side_name):
        side_price_arr = np.array(side_info.price).astype(np.float64)
        try:
            assert len(side_price_arr) > 0
        except AssertionError:
            self.log.error('Empty {side_name} price arr for {symbol}!')
            traceback.print_exc()
            
        side_volume_arr = np.array(side_info.volume).astype(np.float64)
        side_level_arr = np.array(side_info.level).astype(np.int64)
        
        try:
            assert (all(np.diff(side_level_arr) > 0) if side_name == 'bid'
                    else all(np.diff(side_level_arr) < 0))
        except AssertionError:
            traceback.print_exc()
        
        setattr(self, f'_{side_name}_price', side_price_arr)
        setattr(self, f'_{side_name}_volume', side_volume_arr)
        setattr(self, f'_{side_name}_level', side_level_arr)
        
        self._check_valid(side_name)
        
    def _check_valid(self, side):
        valid_idx = getattr(self, f'_{side}_volume') > MINIMUM_SIZE_FILTER
        for data_type in ('price', 'volume', 'level'):
            setattr(self, f'_{side}_{data_type}', getattr(self, f'_{side}_{data_type}')[valid_idx])
    
    def load_tick_size(self, tick_size):
        self.tick_size = tick_size
        
    @property
    def price(self):
        return {'bid': self._bid_price, 'ask': self._ask_price}
    
    @property
    def volume(self):
        return {'bid': self._bid_volume, 'ask': self._ask_volume}
    
    @property
    def level(self):
        return {'bid': self._bid_level, 'ask': self._ask_level}

    @cached_property
    def best_price(self):
        return {'bid': self.price['bid'][0], 'ask': self.price['ask'][-1]}
    
    @cached_property
    def mid_price(self):
        return (self.best_price['bid'] + self.best_price['ask']) / 2
    
    @cached_property
    def price_pct(self):
        mpc = self.mid_price
        bid_price_pct = (mpc - self.price['bid']) / mpc
        ask_price_pct = (self.price['ask'] - mpc) / mpc
        return {'bid': bid_price_pct, 'ask': ask_price_pct}
    
    @cached_property
    def side_amt(self):
        bid_amt = self.price['bid'] * self.volume['bid']
        ask_amt = self.price['ask'] * self.volume['ask']
        return {'bid': bid_amt, 'ask': ask_amt}
    
    @cached_property
    def all_amt(self):
        bid_amt, ask_amt = self.side_amt['bid'], self.side_amt['ask']
        return np.concatenate((bid_amt, ask_amt))
    
    @cached_property
    def all_amt_mean(self):
        return np.mean(self.all_amt)
    
    @cached_property
    def all_amt_std(self):
        return np.std(self.all_amt)
    
    @cached_property
    def prices_sorted_by_level(self):
        bid_price_sorted = self.price['bid'][::-1]
        ask_price_sorted = self.price['ask']
        return {'bid': bid_price_sorted, 'ask': ask_price_sorted}
    
    @cached_property
    def prices_pct_by_level(self):
        bid_sorted, ask_sorted = self.prices_sorted_by_level['bid'], self.prices_sorted_by_level['ask']
        bid1, ask1 = self.best_price['bid'], self.best_price['ask']
        bid_pct = (bid1 - bid_sorted) / bid1
        ask_pct = (ask_sorted - ask1) / ask1
        return {'bid': bid_pct, 'ask': ask_pct}
    
    @cached_property
    def prices_layer_by_level(self):
        bid_sorted, ask_sorted = self.prices_sorted_by_level['bid'], self.prices_sorted_by_level['ask']
        bid_layer = np.arange(bid_sorted.size).astype(np.float64)
        ask_layer = np.arange(ask_sorted.size).astype(np.float64)
        return {'bid': bid_layer, 'ask': ask_layer}
    
    @cached_property
    def prices_tick_by_level(self):
        bid_sorted, ask_sorted = self.prices_sorted_by_level['bid'], self.prices_sorted_by_level['ask']
        bid1, ask1 = self.best_price['bid'], self.best_price['ask']
        bid_tick = (bid1 - bid_sorted) / self.tick_size
        ask_tick = (ask_sorted - ask1) / self.tick_size
        return {'bid': bid_tick, 'ask': ask_tick}
    
    @cached_property
    def total_amt_sum(self):
        return {'bid': np.sum(self.side_amt['bid']),
                'ask': np.sum(self.side_amt['ask'])}
    
    @cached_property
    def amt_sorted_by_level(self):
        bid_amt_sorted = self.side_amt['bid'][::-1]
        ask_amt_sorted = self.side_amt['ask']
        return {'bid': bid_amt_sorted, 'ask': ask_amt_sorted}
    
    @cached_property
    def amt_ratio_sorted_by_level(self):
        return {'bid': self.amt_sorted_by_level['bid']/self.total_amt_sum['bid'], 
                'ask': self.amt_sorted_by_level['ask']/self.total_amt_sum['ask']}
    
    @cached_property
    def amt_cum_ratio_sorted_by_level(self):
        return {'bid': np.cumsum(self.amt_ratio_sorted_by_level['bid']), 
                'ask': np.cumsum(self.amt_ratio_sorted_by_level['ask'])}
    
    def get_price_range_idx_by_side(self, side, pct, range_type):
        if range_type == 'in':
            return self.price_pct[side] <= pct
        elif range_type == 'out':
            return self.price_pct[side] > pct

    def get_price_range_idx(self, pct, range_type):
        return {'bid': self.get_price_range_idx_by_side('bid', pct, range_type),
                'ask': self.get_price_range_idx_by_side('ask', pct, range_type)}

    def get_if_ticktimes_by_side(self, side, multiplier):
        return if_ticktimes(getattr(self, f'{side}_price'), self.tick_size, multiplier)
    
    def get_if_ticktimes(self, multiplier):
        return {'bid': self.get_if_ticktimes_by_side('bid', multiplier),
                'ask': self.get_if_ticktimes_by_side('ask', multiplier)}
    
    def get_n_sigma_thres(self, n):
        return self.all_amt_mean + n * self.all_amt_std
    
    def get_gt_n_sigma_idx(self, n):
        thres = self.get_n_sigma_thres(n)
        return {'bid': self.side_amt['bid'] > thres, 'ask': self.side_amt['ask'] > thres}
    
    def get_lt_n_sigma_idx(self, n):
        thres = self.get_n_sigma_thres(n)
        return {'bid': self.side_amt['bid'] <= thres, 'ask': self.side_amt['ask'] <= thres}
    
    def get_range_idx_on_sorted(self, gt=None, lt=None):
        return {'bid': self.get_range_idx_on_sorted_by_side('bid', gt=gt, lt=lt),
                'ask': self.get_range_idx_on_sorted_by_side('ask', gt=gt, lt=lt)}
            
    def get_range_idx_on_sorted_by_side(self, side, gt=None, lt=None):
        if gt is not None and lt is not None:
            return (self.prices_pct_by_level[side] >= gt) & (self.prices_pct_by_level[side] <= lt)
        elif gt is not None:
            return self.prices_pct_by_level[side] > gt
        elif lt is not None:
            return self.prices_pct_by_level[side] <= lt
        
        
# %% size bar
class Side(Enum):
    BA = 'B'
    SA = 'S'
    A = 'all'
    
    
class Size(Enum):
    S = 'small'
    M = 'mid'
    L = 'large'
    X = 'x_large'
    

class SizeDiv(Enum):
    Quantile = 'quantile'
    Std = 'std'
    
    
class SizeBarProcessor(Processor):

    def __init__(self, pb_msg, log=None):
        super().__init__(pb_msg, log=log)
        self.pb_msg = pb_msg
        self.timestamp = pb_msg.timestamp # !!!: 未确认，可能需要修改
        
    def get(self, side, volume_type, size, size_div):
        target_v = 0
        size_list = list(size) if size is not None else [s.name for s in Size]
        size_div_list = [size_div] if size_div is not None else [sd.name for sd in SizeDiv]
        for size_div_ in size_div_list:
            target_cluster = getattr(self.pb_msg, f'size_bar_clusters_{SizeDiv[size_div_].value}')
            for size_ in size_list:
                size_bar_name = f'{Side[side].value}_{Size[size_].value}_size'
                size_bar = getattr(target_cluster, size_bar_name)
                target_v += getattr(size_bar, volume_type)
        return target_v