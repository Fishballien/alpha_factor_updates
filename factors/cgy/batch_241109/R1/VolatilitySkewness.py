import numpy as np

def VolatilitySkewness(lob: np.array) -> float:
    # 获取最优价（第一个价格）
    best_price = lob[0, 0]

    # 初始化变量用于计算偏度
    price_diffs = []
    N = lob.shape[0]  # 总深度数

    # 逐深度计算价格差归一化后的立方
    for i in range(N):
        current_price = lob[i, 0]
        normalized_price_diff = (current_price - best_price) / best_price
        price_diffs.append(normalized_price_diff ** 3)

    # 计算价格差的偏度
    skewness = np.mean(price_diffs)

    return skewness
