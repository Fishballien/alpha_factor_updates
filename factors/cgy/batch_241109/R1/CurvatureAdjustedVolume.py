import numpy as np

def CurvatureAdjustedVolume(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算价格的二阶导数（价格曲率），形状为 [深度-2]
    price_curvature = np.abs(np.diff(prices, n=2))

    # 取中间位置的成交量，与曲率的形状对齐，形状为 [深度-2]
    adjusted_volumes = volumes[1:-1]

    # 计算加权的曲率成交量
    curvature_adjusted_volume = np.sum(adjusted_volumes * price_curvature)

    return curvature_adjusted_volume
