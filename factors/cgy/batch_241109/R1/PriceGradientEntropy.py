import numpy as np

def PriceGradientEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gradients = np.abs(np.diff(prices)) / (np.abs(prices[:-1]) + 1e-9)
    total_gradient = np.sum(price_gradients)
    if total_gradient == 0:
        return 0
    gradient_probs = price_gradients / total_gradient
    entropy = -np.sum(gradient_probs * np.log(gradient_probs + 1e-9))
    return entropy
