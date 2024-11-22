import numpy as np

def CumulativeGapEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gaps = np.diff(prices)
    cumulative_gaps = np.cumsum(price_gaps)
    total_gap = cumulative_gaps[-1] if cumulative_gaps[-1] != 0 else 1e-9
    gap_probs = cumulative_gaps / total_gap
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
