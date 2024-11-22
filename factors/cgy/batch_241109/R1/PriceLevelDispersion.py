import numpy as np

def PriceLevelDispersion(lob: np.array) -> float:
    # 提取成交量和深度索引
    volumes = lob[:, 1]
    V_total = np.sum(volumes)

    # 如果总成交量为 0，则无法计算分散度
    if V_total == 0:
        return np.nan

    # 获取价格层的索引（深度）
    indices = np.arange(1, lob.shape[0] + 1)

    # 计算加权平均索引
    weighted_mean_index = np.sum(indices * volumes) / V_total

    # 计算分散度
    dispersion = np.sqrt(np.sum(((indices - weighted_mean_index) ** 2) * volumes) / V_total)

    return dispersion
