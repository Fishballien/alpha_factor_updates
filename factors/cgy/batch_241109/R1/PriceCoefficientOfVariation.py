import numpy as np

def PriceCoefficientOfVariation(lob: np.array) -> float:
    # 提取价格，形状为 [深度]
    prices = lob[:, 0]

    # 计算平均价格
    mean_price = np.mean(prices)

    # 计算价格标准差
    std_price = np.std(prices)

    # 处理均值为 0 的情况，避免除以 0
    coef_variation = std_price / mean_price if mean_price != 0 else np.nan

    return coef_variation
