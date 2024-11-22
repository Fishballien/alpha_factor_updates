import numpy as np

def id2_2(ask_factor: np.array, bid_factor: np.array) -> np.array:
    return np.abs(ask_factor - bid_factor) / (np.abs(ask_factor) + np.abs(bid_factor))
