import numpy as np

def CumulativeReciprocalVolume(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 计算倒数成交量，处理除零情况
    with np.errstate(divide='ignore'):
        reciprocal_volumes = 1 / volumes

    # 去除无穷大值（由于 1/0 的情况）
    reciprocal_volumes = np.where(np.isfinite(reciprocal_volumes), reciprocal_volumes, 0)

    # 计算累积倒数成交量
    total_reciprocal = reciprocal_volumes.sum()

    return total_reciprocal
