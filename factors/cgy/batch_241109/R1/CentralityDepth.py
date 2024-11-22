import numpy as np

def CentralityDepth(lob: np.array) -> float:
    # 提取价格数组
    prices = lob[:, 0]

    # 计算均价
    mean_price = np.mean(prices)

    # 计算价格差异的绝对值之和
    absolute_diff_sum = 0.0
    for price in prices:
        absolute_diff_sum += abs(price - mean_price)

    # 防止除零错误
    if absolute_diff_sum == 0:
        return np.nan

    # 计算中心度
    centrality = 1 / (absolute_diff_sum + 1e-9)

    return centrality
