import numpy as np

def PriceRelativeEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    relative_prices = np.abs(prices - best_price) / (np.sum(np.abs(prices - best_price)) + 1e-9)
    entropy = -np.sum(relative_prices * np.log(relative_prices + 1e-9))
    return entropy
