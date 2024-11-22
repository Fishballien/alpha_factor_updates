import numpy as np

def WeightedGapMeanEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gaps = np.abs(np.diff(prices))
    mean_gap = np.mean(price_gaps)
    gap_diffs = np.abs(price_gaps - mean_gap)
    total_diff = np.sum(gap_diffs)
    if total_diff == 0:
        return 0
    gap_probs = gap_diffs / total_diff
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
