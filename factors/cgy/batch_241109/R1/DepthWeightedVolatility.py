import numpy as np

def DepthWeightedVolatility(lob: np.array) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算价格的均值
    mean_price = np.mean(prices)

    # 初始化加权波动率的累积和
    weighted_volatility_sum = 0.0

    # 遍历每个深度位置计算波动率
    for i in range(lob.shape[0]):
        # 计算当前深度位置的波动平方
        volatility_contribution = volumes[i] * (prices[i] - mean_price) ** 2

        # 累加到总波动率
        weighted_volatility_sum += volatility_contribution

    # 计算最终的加权波动率
    weighted_volatility = np.sqrt(weighted_volatility_sum)

    return weighted_volatility
