import numpy as np

def WeightedPriceSpread(lob: np.array) -> float:
    # 获取最优价（第一个价格）
    P_best = lob[0, 0]

    # 初始化总价格和总成交量
    total_weighted_price = 0.0
    total_volume = 0.0

    # 遍历每个深度计算加权价格和总成交量
    for i in range(lob.shape[0]):
        price = lob[i, 0]
        volume = lob[i, 1]

        # 计算加权价格
        total_weighted_price += price * volume
        total_volume += volume

    # 检查是否总成交量为零以避免除零错误
    if total_volume == 0:
        return np.nan

    # 计算加权平均价格
    weighted_price = total_weighted_price / total_volume

    # 计算加权价格差
    weighted_price_spread = weighted_price - P_best

    return weighted_price_spread
