import numpy as np

def DifferentialPriceGradientEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gradients = np.diff(prices)
    gradient_diffs = np.abs(np.diff(price_gradients))
    total_diff = np.sum(gradient_diffs)
    if total_diff == 0:
        return 0
    gradient_probs = gradient_diffs / total_diff
    entropy = -np.sum(gradient_probs * np.log(gradient_probs + 1e-9))
    return entropy
