import numpy as np

def LogCumulativeDepth(lob: np.array) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价和最远价
    best_price = prices[0]
    farthest_price = prices[-1]

    # 计算累积成交量的对数
    log_cumulative_depth = np.log(np.sum(volumes)) * (farthest_price - best_price)

    return log_cumulative_depth
