import numpy as np

def id0_2(ask_factor: np.array, bid_factor: np.array) -> np.array:
    return (ask_factor - bid_factor)/ np.maximum(ask_factor, bid_factor)
