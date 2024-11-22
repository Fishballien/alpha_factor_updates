import numpy as np

def PolyVolumeDiff(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算最优价（第一个档位的价格）
    best_price = prices[0]

    # 设置多项式阶数k
    k = 2

    # 计算多项式加权的成交量差
    poly_volume_diff = np.sum(volumes * (prices - best_price)**k)

    return poly_volume_diff
