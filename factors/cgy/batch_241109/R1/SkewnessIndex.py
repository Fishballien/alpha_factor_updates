import numpy as np

def SkewnessIndex(lob: np.array) -> float:
    # 获取最优价（第一个价格）
    P_best = lob[0, 0]

    # 提取所有价格
    prices = lob[:, 0]

    # 计算中位数价格
    P_median = np.median(prices)

    # 计算卖单偏度指数
    ask_skewness_index = (P_best - P_median) / P_best if P_best != 0 else np.nan

    return ask_skewness_index
