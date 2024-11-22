import numpy as np

def PriceVolatilityClustering(lob: np.array) -> float:
    # 提取价格
    prices = lob[:, 0]

    # 计算平均价格
    mean_price = np.mean(prices)

    # 初始化分子和分母
    numerator = 0.0
    denominator = 0.0

    # 遍历每个价格计算波动性聚集度
    for i in range(lob.shape[0]):
        deviation = prices[i] - mean_price
        numerator += deviation ** 2
        denominator += abs(deviation)

    # 避免分母为零
    if denominator == 0:
        return np.nan

    # 计算聚集度
    clustering = numerator / denominator

    return clustering
