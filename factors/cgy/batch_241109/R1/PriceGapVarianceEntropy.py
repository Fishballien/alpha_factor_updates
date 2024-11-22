import numpy as np

def PriceGapVarianceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gaps = np.diff(prices)
    variance = np.var(price_gaps)
    variance_probs = np.abs(price_gaps - variance) / (np.sum(np.abs(price_gaps - variance)) + 1e-9)
    entropy = -np.sum(variance_probs * np.log(variance_probs + 1e-9))
    return entropy
