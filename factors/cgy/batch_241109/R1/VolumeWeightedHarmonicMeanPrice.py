import numpy as np

def VolumeWeightedHarmonicMeanPrice(lob: np.array) -> float:
    # 初始化分子和分母
    numerator = 0.0
    denominator = 0.0

    # 遍历每个深度
    for i in range(lob.shape[0]):
        price = lob[i, 0]
        volume = lob[i, 1]

        # 累加成交量到分子
        numerator += volume

        # 累加成交量/价格到分母，避免价格为0的情况
        if price != 0:
            denominator += volume / price

    # 计算调和平均价格，防止分母为0
    harmonic_mean_price = numerator / denominator if denominator != 0 else np.nan

    return harmonic_mean_price
