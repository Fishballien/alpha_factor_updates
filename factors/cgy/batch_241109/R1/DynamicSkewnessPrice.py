import numpy as np

def DynamicSkewnessPrice(lob: np.array) -> float:
    # 提取价格，形状为 [深度]
    prices = lob[:, 0]

    # 计算均值和标准差
    mean_price = np.mean(prices)
    std_price = np.std(prices)
    
    # 避免除零错误
    if std_price == 0:
        return np.nan

    # 计算动态偏度
    skewness = np.mean((prices - mean_price) ** 3) / (std_price ** 3)
    
    return skewness
