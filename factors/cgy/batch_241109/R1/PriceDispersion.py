import numpy as np

def PriceDispersion(lob: np.array) -> float:
    # 提取价格列表
    prices = lob[:, 0]

    # 计算平均价格
    mean_price = np.mean(prices)

    # 初始化离散度累积和
    dispersion_sum = 0.0

    # 遍历每个深度计算绝对偏差
    for price in prices:
        dispersion_sum += abs(price - mean_price)

    # 计算平均离散度
    dispersion = dispersion_sum / len(prices)

    return dispersion
