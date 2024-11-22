import numpy as np

def WeightedSpread(lob: np.array) -> float:
    # 提取最优价（第一个价格）
    best_price = lob[0, 0]

    # 初始化加权价差累加器和总成交量累加器
    weighted_spread_sum = 0.0
    total_volume = 0.0

    # 遍历每个深度
    for i in range(lob.shape[0]):
        # 当前深度的价格和成交量
        current_price = lob[i, 0]
        current_volume = lob[i, 1]

        # 计算价差（当前价格与最优价的差）
        price_diff = current_price - best_price

        # 加权计算并累加
        weighted_spread_sum += price_diff * current_volume
        total_volume += current_volume

    # 防止除以零
    if total_volume == 0:
        return np.nan

    # 计算加权价差
    weighted_spread = weighted_spread_sum / total_volume

    return weighted_spread
