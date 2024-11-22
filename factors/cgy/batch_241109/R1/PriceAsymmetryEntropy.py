import numpy as np

def PriceAsymmetryEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    best_price = prices[0]
    upper_half = prices[prices >= best_price]
    lower_half = prices[prices < best_price]
    upper_probs = np.abs(upper_half - best_price) / (np.sum(np.abs(upper_half - best_price)) + 1e-9)
    lower_probs = np.abs(lower_half - best_price) / (np.sum(np.abs(lower_half - best_price)) + 1e-9)
    entropy = -np.sum(upper_probs * np.log(upper_probs + 1e-9)) - np.sum(lower_probs * np.log(lower_probs + 1e-9))
    return entropy
