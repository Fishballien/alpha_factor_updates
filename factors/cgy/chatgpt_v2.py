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
from concurrent.futures import ProcessPoolExecutor, as_completed
import toml
import importlib
from datetime import timedelta
import traceback
import argparse
import gc


# %% add sys path
file_path = Path(__file__).resolve()
file_dir = file_path.parents[0]
project_dir = file_path.parents[2]
sys.path.append(str(project_dir))


# %%
from core.factor_updater import FactorUpdaterTsFeatureOfSnapsTest
from core.cache_persist_manager import CacheManager, GeneralPersistenceMgr
from core.immediate_process_manager import (ImmediateLevelManagerFromDict, LevelProcessorForChatgptV0,
                                            extract_arrays_from_pb_msg)
from utils.decorator_utils import timeit
from utils.datautils import align_series_index
from utils.timeutils import parse_time_string
from receiver.rcv_fr_lord import LordSaveToDict


# %% cache & persist
class MyCacheMgr(CacheManager):
    
    def init_cache_mapping(self):
        final_factors = self.params['factors_related']['r1']
        max_levels = final_factors['max_levels']
        max_pcts = final_factors['max_pcts']
        cache_mapping = {}
        cache_mapping.update({
            (side, 'level', max_level): f'{side}_level_{max_level}'
            for side in ('bid', 'ask')
            for max_level in max_levels
            })
        cache_mapping.update({
            (side, 'pct', pct): f'{side}_pct_{pct}'
            for side in ('bid', 'ask')
            for pct in max_pcts
            })
        self.cache_mapping = cache_mapping
        
        
# %% immediate process
# def process_snapshot(*args, r1_func, max_levels, max_pcts):
#     lp = LevelProcessorForChatgptV0(*args)
#     sides = ('bid', 'ask')
    
#     results = {}
    
#     for max_level in max_levels:
#         lob_within_level = lp.lob_within_level(max_level)
#         for side in sides:
#             if len(lob_within_level[side]) == 0:
#                 continue
#             results[(side, 'level', max_level)] = r1_func(lob_within_level[side])
            
#     for pct in max_pcts:
#         lob_within_pct = lp.lob_within_pct(pct)
#         for side in sides:
#             if len(lob_within_pct[side]) == 0:
#                 continue
#             results[(side, 'pct', pct)] = r1_func(lob_within_pct[side])
    
#     return results


# class MyImmediateProcessMgr(ImmediateLevelManager):

#     def load_info(self, param, factor_name):
#         self.param = param
        
#         factors_related = self.param['factors_related']
#         final_factors = factors_related['r1']
#         self.max_levels = final_factors['max_levels']
#         self.max_pcts = final_factors['max_pcts']
        
#         module = self.param['func_module']
#         r1_module_name = f'{module}.R1.{factor_name}'
#         r1_module = importlib.import_module(r1_module_name)
#         self.r1_func = getattr(r1_module, factor_name)

#     def _init_topic_func_mapping(self):
#         self.topic_func_mapping['CCLevel1'] = self._process_cc_level_msg
    
#     def get_one_snapshot(self, ts, min_lob):
# # =============================================================================
# #         from tqdm import tqdm
# #         for symbol, p in tqdm(list(self.container.items()), desc='get_snapshot'):
# #             try:
# #                 pb_msg = p.pb_msg
# #                 ts = p.ts
# #                 arrays = extract_arrays_from_pb_msg(pb_msg)
# #         
# #                 # 直接调用 process_snapshot 函数而不是提交到线程池
# #                 results = process_snapshot(*arrays,
# #                                             r1_func=self.r1_func,
# #                                             max_levels=self.max_levels,
# #                                             max_pcts=self.max_pcts)
# #                 
# #                 # 处理结果
# #                 for key, f in results.items():
# #                     self.factor[key][symbol] = f
# #         
# #                 # 更新时间戳
# #                 self.update_time[symbol] = ts
# #         
# #             except Exception as exc:
# #                 traceback.print_exc()
# #                 self.log.error(f"Snapshot processing generated an exception: {exc}")
# # =============================================================================

#         with ProcessPoolExecutor(max_workers=5) as executor:
#             futures = {}
#             for symbol, p in list(min_lob.items()):
#                 pb_msg = p.pb_msg
#                 ts = p.ts
#                 arrays = extract_arrays_from_pb_msg(pb_msg)

#                 future = executor.submit(process_snapshot, *arrays, 
#                                          r1_func=self.r1_func,
#                                          max_levels=self.max_levels,
#                                          max_pcts=self.max_pcts)
#                 futures[future] = (symbol, ts)

#             for future in as_completed(futures):
#                 try:
#                     results = future.result()
#                     symbol, ts = futures[future]

#                     for key, f in results.items():
#                         self.factor[key][symbol] = f

#                     self.update_time[symbol] = ts

#                 except Exception as exc:
#                     traceback.print_exc()
#                     self.log.error(f"Snapshot processing generated an exception: {exc}")
#         gc.collect()


# %% immediate process
def process_snapshot(*args, r1_func, max_levels, max_pcts):
    try:
        lp = LevelProcessorForChatgptV0(*args)
        sides = ('bid', 'ask')
        results = {}

        for max_level in max_levels:
            lob_within_level = lp.lob_within_level(max_level)
            for side in sides:
                if len(lob_within_level[side]) == 0:
                    continue
                results[(side, 'level', max_level)] = r1_func(lob_within_level[side])

        for pct in max_pcts:
            lob_within_pct = lp.lob_within_pct(pct)
            for side in sides:
                if len(lob_within_pct[side]) == 0:
                    continue
                results[(side, 'pct', pct)] = r1_func(lob_within_pct[side])

        return results

    except Exception as e:
        traceback.print_exc()
        return {}  # 出错就返回空字典，主进程判断处理

    finally:
        gc.collect()  # 子进程主动清理


class MyImmediateProcessMgr(ImmediateLevelManagerFromDict):

    def load_info(self, param, factor_name):
        self.param = param

        factors_related = self.param['factors_related']
        final_factors = factors_related['r1']
        self.max_levels = final_factors['max_levels']
        self.max_pcts = final_factors['max_pcts']

        module = self.param['func_module']
        r1_module_name = f'{module}.R1.{factor_name}'
        r1_module = importlib.import_module(r1_module_name)
        self.r1_func = getattr(r1_module, factor_name)

    def _init_topic_func_mapping(self):
        self.topic_func_mapping['CCLevel'] = self._process_cc_level_msg

    def get_one_snapshot(self, ts, min_lob):
        
        return
        with ProcessPoolExecutor(max_workers=5) as executor:
            futures = {}
            for symbol, pb_msg in list(min_lob.items()):
                arrays = extract_arrays_from_pb_msg(pb_msg)

                future = executor.submit(
                    process_snapshot,
                    *arrays,
                    r1_func=self.r1_func,
                    max_levels=self.max_levels,
                    max_pcts=self.max_pcts
                )
                futures[future] = (symbol, ts)

            for future in as_completed(futures):
                symbol, ts = futures[future]
                results = future.result()

                if not results:
                    self.log.warning(f"Empty result for {symbol} at {ts}")
                    continue

                for key, f in results.items():
                    self.factor[key][symbol] = f

                self.update_time[symbol] = ts


# %%
class FChatgptV1(FactorUpdaterTsFeatureOfSnapsTest):

    def __init__(self, factor_name, config_name):
        self.name = factor_name
        self.config_name = config_name
        super().__init__()

        self._init_r2_funcs()
        
    def _init_msg_controller(self):
        self.msg_controller = LordSaveToDict(self.topic_list, address=self.address, log=self.log)
        
    def _init_dir(self):
        path_config = self.path_config
        
        self.param_dir = Path(path_config['param'])
        cache_dir = Path(path_config['cache'])
        persist_dir = Path(path_config['persist'])
        self.cache_dir = cache_dir / self.config_name / self.name 
        self.persist_dir = persist_dir / self.config_name / self.name
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.persist_dir.mkdir(parents=True, exist_ok=True)
           
    def _load_param(self):
        self.params = toml.load(self.param_dir / f'{self.config_name}.toml')
        
    def _init_lookback_mapping(self):
        cache_period = self.params['record']['cache_period']
        self.cache_lookback = timedelta(seconds=parse_time_string(cache_period))
        
    def _init_r2_funcs(self):
        module = self.params['func_module']
        factors_related = self.params['factors_related']
        r2_list = factors_related['r2']
        self.r2_funcs = {r2_name: getattr(importlib.import_module(f'{module}.R2.{r2_name}'), r2_name) 
                         for r2_name in r2_list}
    
    def _init_param_names(self):
        factor_name = self.name
        factors_related = self.params['factors_related']
        r1 = factors_related['r1']
        max_levels = r1['max_levels']
        max_pcts = r1['max_pcts']
        r2_list = factors_related['r2']
        
        slice_shortcut = {
            'level': 'L',
            'pct': 'P',
            }
        
        self.param_set = []
        for slice_v, slice_type in (list(zip(max_levels, ['level']*len(max_levels))) 
                                    + list(zip(max_pcts, ['pct']*len(max_pcts)))):
            for side in ('bid', 'ask'):
                name = f"{factor_name}_{side}_{slice_shortcut[slice_type]}{slice_v}"
                self.param_set.append({
                    'slice_type': slice_type,
                    'slice_v': slice_v,
                    'r2': side,
                    'name': name,
                    })
            for r2_name in r2_list:
                name = f"{factor_name}_{slice_shortcut[slice_type]}{slice_v}_R2_{r2_name}"
                self.param_set.append({
                    'slice_type': slice_type,
                    'slice_v': slice_v,
                    'r2': r2_name,
                    'name': name,
                    })
        
    def _init_managers(self):
        # 即时记录
        self.immediate_mgr = MyImmediateProcessMgr(self.topic_list, self.msg_controller, log=self.log)
        self.immediate_mgr.load_info(self.params, self.name)
        # 定时记录
        self.cache_mgr = MyCacheMgr(self.params, self.param_set, self.cache_dir, 
                                    self.cache_lookback, file_name='cache', log=self.log)
        self.persist_mgr = GeneralPersistenceMgr(self.params, self.param_set, self.persist_dir, log=self.log)

    @timeit
    def _final_calc(self, ts):
        temp_dict = {}
        for pr in self.param_set:
            slice_type = pr['slice_type']
            slice_v = pr['slice_v']
            r2_name = pr['r2']
            pr_name = pr['name']
            if r2_name in ['bid', 'ask']:
                factor_final = self.cache_mgr[(r2_name, slice_type, slice_v)].iloc[-1]
            else:
                factor_side = {side: self.cache_mgr[(side, slice_type, slice_v)].iloc[-1]
                               for side in ['bid', 'ask']}
                factor_side['bid'], factor_side['ask'] = align_series_index(factor_side['bid'], factor_side['ask'])
                r2_func = self.r2_funcs[r2_name]
                factor_final = r2_func(factor_side['ask'], factor_side['bid'])
            r_level = 'r1' if r2_name in ['bid', 'ask'] else 'r2'
            # self.db_handler.batch_insert_data(self.author, self.category[r_level], pr_name, factor_final, ts)
            temp_dict[pr_name] = {'category': self.category[r_level], 'factor_final': factor_final}
        return temp_dict
    
        
# %%
if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--factor_name', type=str, help='factor_name')
    parser.add_argument('-b', '--batch_name', type=str, help='batch_name')

    args = parser.parse_args()
    
    # factor_name = 'AbsolutePriceGradient'
    # batch_name = 'batch_241109'
    
    factor_name = args.factor_name
    batch_name = args.batch_name
    
    updater = FChatgptV1(factor_name, batch_name)
    updater.run()
        