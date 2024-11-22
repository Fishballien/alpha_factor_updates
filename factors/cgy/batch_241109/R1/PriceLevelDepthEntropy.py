import numpy as np

def PriceLevelDepthEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    depth_levels = np.arange(1, lob.shape[0] + 1)
    depth_probabilities = depth_levels / np.sum(depth_levels)
    entropy = -np.sum(depth_probabilities * np.log(depth_probabilities + 1e-9))
    return entropy
