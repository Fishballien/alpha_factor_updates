import numpy as np

def LogarithmicGapEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    price_gaps = np.log1p(np.abs(np.diff(prices)))  # 计算价格层之间的对数间隔
    total_gap = np.sum(price_gaps)
    if total_gap == 0:
        return 0
    gap_probs = price_gaps / total_gap
    entropy = -np.sum(gap_probs * np.log(gap_probs + 1e-9))
    return entropy
