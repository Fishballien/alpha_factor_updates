import numpy as np

def InvertedPriceGapEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gaps = 1 / (np.abs(np.diff(prices)) + 1e-9)
    total_inverted_gap = np.sum(price_gaps)
    if total_inverted_gap == 0:
        return 0
    gap_probs = price_gaps / total_inverted_gap
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
