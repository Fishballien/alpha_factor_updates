import numpy as np

def PriceSlope(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算累计成交量
    cumulative_volumes = np.cumsum(volumes)

    # 检查是否有足够的不同点进行拟合
    if len(cumulative_volumes) < 2 or np.all(cumulative_volumes == cumulative_volumes[0]):
        return np.nan

    # 计算线性回归斜率（多项式拟合一阶）
    slope = np.polyfit(cumulative_volumes, prices, 1)[0]

    return slope
