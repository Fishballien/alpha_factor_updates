import numpy as np

def WeightedCumulativePriceDeviation(lob: np.array) -> float:
    # 初始化累积变量
    cumulative_volume = 0.0
    cumulative_price_sum = 0.0
    total_deviation = 0.0

    # 遍历每个深度进行计算
    for i in range(lob.shape[0]):
        price = lob[i, 0]
        volume = lob[i, 1]

        # 更新累积成交量和累积价格
        cumulative_volume += volume
        cumulative_price_sum += price * volume

        # 避免除以零
        if cumulative_volume != 0:
            cumulative_avg_price = cumulative_price_sum / cumulative_volume
        else:
            cumulative_avg_price = 0.0

        # 计算偏差并进行加权累加
        deviation = (price - cumulative_avg_price) * volume
        total_deviation += deviation

    return total_deviation
