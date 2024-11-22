import numpy as np

def GaussianPriceLevelEntropy(lob: np.array, sigma: float = 0.1) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    gaussian_weights = np.exp(-((prices - best_price) ** 2) / (2 * sigma ** 2))
    total_weighted_price = np.sum(gaussian_weights)
    if total_weighted_price == 0:
        return 0
    price_probs = gaussian_weights / total_weighted_price
    entropy = -np.sum(price_probs * np.log(price_probs + 1e-9))
    return entropy
