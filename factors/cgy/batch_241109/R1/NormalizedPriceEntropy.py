import numpy as np

def NormalizedPriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    normalized_prices = (prices - np.min(prices)) / (np.max(prices) - np.min(prices) + 1e-9)
    entropy = -np.sum(normalized_prices * np.log(normalized_prices + 1e-9))
    return entropy
