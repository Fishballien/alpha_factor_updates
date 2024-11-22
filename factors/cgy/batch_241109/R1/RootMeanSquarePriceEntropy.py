import numpy as np

def RootMeanSquarePriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    rms_price = np.sqrt(np.mean(prices ** 2))
    price_diffs = np.abs(prices - rms_price)
    total_diff = np.sum(price_diffs)
    if total_diff == 0:
        return 0
    price_probs = price_diffs / total_diff
    entropy = -np.sum(price_probs * np.log(price_probs + 1e-9))
    return entropy
