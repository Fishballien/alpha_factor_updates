import numpy as np

def RelativeQuadraticDepth(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价 (即第一个卖单的价格)
    best_price = prices[0]

    # 计算相对价格平方偏差
    relative_price_diff_squared = ((prices - best_price) / best_price) ** 2

    # 计算加权平方偏差
    quadratic_depth = np.sum(relative_price_diff_squared * volumes)

    return quadratic_depth
