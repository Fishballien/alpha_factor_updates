import numpy as np

def PriceGapEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gaps = np.diff(prices)
    total_gap = np.sum(price_gaps)
    if total_gap == 0:
        return 0  # 总间隔为零时返回零熵
    probabilities = price_gaps / total_gap
    entropy = -np.sum(probabilities * np.log(probabilities + 1e-9))  # 防止 log(0)
    return entropy
