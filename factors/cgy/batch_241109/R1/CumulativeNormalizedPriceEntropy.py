import numpy as np

def CumulativeNormalizedPriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    cumulative_prices = np.cumsum(prices)
    normalized_prices = cumulative_prices / (cumulative_prices[-1] + 1e-9)
    entropy = -np.sum(normalized_prices * np.log(normalized_prices + 1e-9))
    return entropy
