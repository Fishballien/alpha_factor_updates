import numpy as np

def VolatilityWeightedPriceDispersion(lob: np.array) -> float:
    # 获取最优价（第一个价格）
    P_best = lob[0, 0]

    # 初始化加权离散度的累加器
    weighted_dispersion_sum = 0.0
    volume_sum = 0.0

    # 遍历每个深度计算加权价格离散度
    for i in range(lob.shape[0]):
        price = lob[i, 0]
        volume = lob[i, 1]

        # 计算平方的价格差异
        price_diff_squared = (price - P_best) ** 2

        # 累加加权的价格差异和成交量
        weighted_dispersion_sum += price_diff_squared * volume
        volume_sum += volume

    # 计算最终的加权价格离散度
    if volume_sum == 0:
        return np.nan

    weighted_dispersion = weighted_dispersion_sum / volume_sum

    return weighted_dispersion
