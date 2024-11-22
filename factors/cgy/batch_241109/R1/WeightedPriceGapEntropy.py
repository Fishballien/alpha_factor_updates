import numpy as np

def WeightedPriceGapEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]
    price_gaps = np.abs(np.diff(prices))
    avg_volumes = (volumes[:-1] + volumes[1:]) / 2
    weighted_gaps = price_gaps * avg_volumes
    total_weighted_gap = np.sum(weighted_gaps)
    if total_weighted_gap == 0:
        return 0
    gap_probs = weighted_gaps / total_weighted_gap
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
