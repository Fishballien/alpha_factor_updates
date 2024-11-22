import numpy as np

def ExponentialPriceWeightedEntropy(lob: np.array, decay: float = 0.05) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    price_weights = np.exp(-decay * np.abs(prices - best_price))
    total_weighted_price = np.sum(price_weights)
    if total_weighted_price == 0:
        return 0
    price_probs = price_weights / total_weighted_price
    entropy = -np.sum(price_probs * np.log(price_probs + 1e-9))
    return entropy
