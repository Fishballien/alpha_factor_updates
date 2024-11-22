import numpy as np

def ExponentialLiquiditySkewness(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价（第一个价位）
    best_price = prices[0]

    # 计算加权均价
    weighted_mean_price = np.sum(prices * volumes) / np.sum(volumes)

    # 计算指数偏斜度
    skewness = np.sum((prices - weighted_mean_price) * volumes * 
                        np.exp((prices - best_price) / weighted_mean_price)) / np.sum(volumes)

    return skewness
