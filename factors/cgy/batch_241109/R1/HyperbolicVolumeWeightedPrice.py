import numpy as np

def HyperbolicVolumeWeightedPrice(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价
    best_price = prices[0]

    # 计算加权双曲差值
    price_diff = prices - best_price
    hvp = np.sum((volumes / (1 + np.abs(price_diff))) * price_diff)

    return hvp
