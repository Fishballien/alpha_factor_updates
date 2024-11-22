import numpy as np

def ExponentialGapDecayEntropy(lob: np.array, decay_factor: float = 0.05) -> float:
    prices = lob[:, 0]
    price_gaps = np.abs(np.diff(prices))
    decay_weights = np.exp(-decay_factor * np.arange(len(price_gaps)))
    weighted_gaps = price_gaps * decay_weights
    total_weighted_gap = np.sum(weighted_gaps)
    if total_weighted_gap == 0:
        return 0
    gap_probs = weighted_gaps / total_weighted_gap
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
