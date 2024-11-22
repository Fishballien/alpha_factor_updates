import numpy as np

def NonlinearWeightedPriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    squared_price_diffs = (prices - best_price) ** 2
    total_squared_diff = np.sum(squared_price_diffs)
    if total_squared_diff == 0:
        return 0
    price_probs = squared_price_diffs / total_squared_diff
    entropy = -np.sum(price_probs * np.log(price_probs + 1e-9))
    return entropy
