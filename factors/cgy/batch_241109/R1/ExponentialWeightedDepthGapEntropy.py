import numpy as np

def ExponentialWeightedDepthGapEntropy(lob: np.array, alpha: float = 0.1) -> float:
    prices = lob[:, 0]
    depth_gaps = np.abs(np.diff(prices))
    decay_weights = np.exp(-alpha * np.arange(len(depth_gaps)))
    weighted_gaps = depth_gaps * decay_weights
    total_weighted_gap = np.sum(weighted_gaps)
    if total_weighted_gap == 0:
        return 0
    gap_probs = weighted_gaps / total_weighted_gap
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
