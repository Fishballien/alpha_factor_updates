import numpy as np

def QuantileSpread(lob: np.array) -> float:
    # 提取价格列
    prices = lob[:, 0]

    # 计算第25百分位和第75百分位
    quantile_25 = np.percentile(prices, 25)
    quantile_75 = np.percentile(prices, 75)

    # 计算分位数差距
    quantile_spread = quantile_75 - quantile_25

    return quantile_spread
