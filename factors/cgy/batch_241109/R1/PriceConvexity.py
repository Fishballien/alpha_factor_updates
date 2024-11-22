import numpy as np

def PriceConvexity(lob: np.array) -> float:
    # 初始化凸度累积和和权重累积和
    convexity_sum = 0.0
    weight_sum = 0.0

    # 遍历每个可计算凸度的位置（忽略开头和结尾）
    for i in range(1, lob.shape[0] - 1):
        # 当前深度的前后价格
        price_prev = lob[i - 1, 0]
        price_curr = lob[i, 0]
        price_next = lob[i + 1, 0]

        # 当前深度的成交量
        volume = lob[i, 1]

        # 计算凸度项：p[i-1] - 2 * p[i] + p[i+1]
        convexity_term = price_prev - 2 * price_curr + price_next

        # 累加凸度和权重
        convexity_sum += convexity_term * volume
        weight_sum += volume

    # 计算加权凸度，如果没有权重则返回 NaN
    return convexity_sum / weight_sum if weight_sum != 0 else np.nan
