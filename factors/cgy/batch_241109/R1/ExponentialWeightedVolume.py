import numpy as np

def ExponentialWeightedVolume(lob: np.array) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算最优价（第一个档位的价格）
    best_price = prices[0]

    # 设置衰减系数 alpha
    alpha = 0.1

    # 计算指数加权的成交量
    exp_weighted_volume = np.sum(volumes * np.exp(-alpha * (prices - best_price)))

    return exp_weighted_volume
