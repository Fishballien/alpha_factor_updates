# -*- coding: utf-8 -*-
"""
Created on Thu Feb  1 13:27:54 2024

@author: Xintang Zheng

"""
# %% imports
import time
import threading
from decorator import decorator


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


@run_by_thread()
@run_every(interval=1)
def func():
    print(time.time())
    time.sleep(0.5)
    
