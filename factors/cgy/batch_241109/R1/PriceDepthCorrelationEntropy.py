import numpy as np

def PriceDepthCorrelationEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    depths = np.arange(1, len(prices) + 1)
    correlation_weights = np.abs(prices - np.mean(prices)) * depths
    total_weight = np.sum(correlation_weights)
    if total_weight == 0:
        return 0
    correlation_probs = correlation_weights / total_weight
    entropy = -np.sum(correlation_probs * np.log(correlation_probs + 1e-9))
    return entropy
