import numpy as np

def SkewnessDepth(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算加权均价
    weighted_mean_price = np.sum(prices * volumes) / np.sum(volumes)
    
    # 计算加权标准差
    price_diff = prices - weighted_mean_price
    weighted_std = np.sqrt(np.sum((price_diff ** 2) * volumes) / np.sum(volumes))

    # 计算偏度
    skewness = np.sum((price_diff ** 3) * volumes) / (len(prices) * (weighted_std ** 3) + 1e-9)  # 防止除零错误

    return skewness
