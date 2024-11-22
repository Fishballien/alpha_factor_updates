import numpy as np

def CumulativeWeightedPrice(lob: np.array) -> float:
    # 初始化加权价格的分子和分母
    weighted_sum = 0.0
    volume_sum = 0.0

    # 遍历每个深度，累加价格的加权和与总成交量
    for i in range(lob.shape[0]):
        price = lob[i, 0]  # 当前深度的价格
        volume = lob[i, 1]  # 当前深度的成交量

        # 计算加权总和和累积的成交量
        weighted_sum += price * volume
        volume_sum += volume

    # 计算最终的加权平均价格，如果总成交量为零，则返回NaN
    weighted_price = weighted_sum / volume_sum if volume_sum != 0 else np.nan
    return weighted_price
