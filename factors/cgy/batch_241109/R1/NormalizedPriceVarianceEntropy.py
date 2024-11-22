import numpy as np

def NormalizedPriceVarianceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_variances = (prices - np.mean(prices)) ** 2
    total_variance = np.sum(price_variances)
    if total_variance == 0:
        return 0
    variance_probs = price_variances / total_variance
    entropy = -np.sum(variance_probs * np.log(variance_probs + 1e-9))
    return entropy
