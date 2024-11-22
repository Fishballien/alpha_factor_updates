import numpy as np

def PriceLevelGapMean(lob: np.array) -> float:
    # 初始化间隔总和和间隔计数
    gap_sum = 0.0
    gap_count = 0

    # 遍历每个深度，计算相邻价格的间隔
    for i in range(1, lob.shape[0]):
        # 当前价格和前一个价格
        current_price = lob[i, 0]
        previous_price = lob[i - 1, 0]

        # 计算价格间隔
        price_gap = current_price - previous_price

        # 累加间隔和间隔数
        gap_sum += price_gap
        gap_count += 1

    # 计算均值价格间隔
    mean_gap = gap_sum / gap_count if gap_count > 0 else np.nan

    return mean_gap
