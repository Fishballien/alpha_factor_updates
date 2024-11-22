import numpy as np

def id0_4(ask_factor: np.array, bid_factor: np.array) -> np.array:
    return (ask_factor + bid_factor) / 2
