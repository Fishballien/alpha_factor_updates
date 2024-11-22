import numpy as np

def PriceMomentum(lob: np.array) -> float:
    # 提取最优价（第一个深度的价格）
    best_price = lob[0, 0]

    # 计算平均价格（遍历计算）
    total_price = 0.0
    num_levels = lob.shape[0]
    for i in range(num_levels):
        total_price += lob[i, 0]

    avg_price = total_price / num_levels if num_levels != 0 else np.nan

    # 计算价格动量
    momentum = (best_price - avg_price) / avg_price if avg_price != 0 else np.nan

    return momentum
