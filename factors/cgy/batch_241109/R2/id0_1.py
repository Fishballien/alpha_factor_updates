import numpy as np

def id0_1(ask_factor: np.array, bid_factor: np.array) -> np.array:
    return ask_factor - bid_factor
