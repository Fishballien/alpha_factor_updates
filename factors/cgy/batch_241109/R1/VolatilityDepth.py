import numpy as np

def VolatilityDepth(lob: np.array) -> float:
    # 提取价格数据
    prices = lob[:, 0]

    # 计算均值
    mean_price = np.mean(prices)

    # 计算方差（逐元素）
    variance_sum = 0.0
    for price in prices:
        variance_sum += (price - mean_price) ** 2

    # 计算标准差（波动率）
    volatility = np.sqrt(variance_sum / (prices.shape[0] - 1))

    return volatility
