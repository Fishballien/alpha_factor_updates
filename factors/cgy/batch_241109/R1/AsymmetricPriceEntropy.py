import numpy as np

def AsymmetricPriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    price_diffs = prices - best_price
    positive_probs = price_diffs[price_diffs > 0] / np.sum(price_diffs[price_diffs > 0] + 1e-9)
    negative_probs = -price_diffs[price_diffs < 0] / np.sum(-price_diffs[price_diffs < 0] + 1e-9)
    entropy = -np.sum(positive_probs * np.log(positive_probs + 1e-9)) - np.sum(negative_probs * np.log(negative_probs + 1e-9))
    return entropy
