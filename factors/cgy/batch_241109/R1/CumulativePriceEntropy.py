import numpy as np

def CumulativePriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    cumulative_prices = np.cumsum(prices)
    total_cumulative = cumulative_prices[-1]
    if total_cumulative == 0:
        return 0
    price_probs = cumulative_prices / total_cumulative
    entropy = -np.sum(price_probs * np.log(price_probs + 1e-9))
    return entropy
