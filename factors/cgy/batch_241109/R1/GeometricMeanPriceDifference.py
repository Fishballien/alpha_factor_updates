import numpy as np

def GeometricMeanPriceDifference(lob: np.array) -> float:
    prices = lob[:, 0]

    # 计算相邻价位的比率
    price_ratio = prices[1:] / prices[:-1]

    # 计算几何平均
    gm_price_diff = np.prod(price_ratio) ** (1 / (len(price_ratio)))

    return gm_price_diff
