import numpy as np

def ExponentialPricePositionEntropy(lob: np.array, beta: float = 0.01) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    position_weights = np.exp(beta * (prices - best_price))
    total_weight = np.sum(position_weights)
    if total_weight == 0:
        return 0
    position_probs = position_weights / total_weight
    entropy = -np.sum(position_probs * np.log(position_probs + 1e-9))
    return entropy
