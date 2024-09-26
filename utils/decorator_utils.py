# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 13:27:54 2024

@author: Xintang Zheng

"""
# %% imports
import time
import threading
from decorator import decorator
import gc
from functools import wraps


# %% decorator
@decorator
def run_every(func, interval=1, *args, **kwargs):
    time.sleep(interval)
    while True:
        start = time.time()
        func(*args, **kwargs)
        duration = (time.time() - start)
        sleep_t = max(interval - duration, 0)
        time.sleep(sleep_t)
        
        
@decorator
def run_by_thread(func, daemon=True, *args, **kwargs):
    thread = threading.Thread(target=func, args=args, kwargs=kwargs)
    thread.daemon = daemon
    thread.start()
    return thread


# %%
def timeit(func):
    """装饰器函数，用于测量函数执行时间"""
    def wrapper(*args, **kwargs):
        start_time = time.time()  # 记录函数开始时间
        result = func(*args, **kwargs)  # 调用函数
        end_time = time.time()  # 记录函数结束时间
        print(f"{func.__name__} ran in {end_time - start_time:.4f} seconds")
        return result
    return wrapper


# %%
def gc_collect_after(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)  # 调用原始函数
        gc.collect()  # 在函数执行后调用垃圾回收
        return result
    return wrapper


# %%
if __name__=='__main__':
    @run_by_thread()
    @run_every(interval=1)
    def func():
        print(time.time())
        time.sleep(0.5)
    func()
        
