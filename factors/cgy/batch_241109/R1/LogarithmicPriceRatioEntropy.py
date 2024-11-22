import numpy as np

def LogarithmicPriceRatioEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_ratios = np.log1p(np.abs(np.diff(prices) / (prices[:-1] + 1e-9)))
    total_ratio = np.sum(price_ratios)
    if total_ratio == 0:
        return 0
    ratio_probs = price_ratios / total_ratio
    entropy = -np.sum(ratio_probs * np.log(ratio_probs + 1e-9))
    return entropy
