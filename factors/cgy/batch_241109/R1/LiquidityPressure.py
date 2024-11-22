import numpy as np

def LiquidityPressure(lob: np.array) -> float:
    # 提取最优价（第一个深度的价格）
    best_price = lob[0, 0]

    # 初始化加权和及价格差和
    weighted_sum = 0.0
    price_sum = 0.0

    # 遍历每个深度位置
    for i in range(lob.shape[0]):
        # 当前深度的价格和成交量
        current_price = lob[i, 0]
        current_volume = lob[i, 1]

        # 计算价格差
        price_diff = current_price - best_price

        # 累加加权和及价格差和
        weighted_sum += current_volume * price_diff
        price_sum += price_diff

    # 返回流动性压力
    return weighted_sum / price_sum if price_sum != 0 else np.nan
