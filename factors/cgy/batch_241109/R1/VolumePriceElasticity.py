import numpy as np

def VolumePriceElasticity(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算价格和成交量的变化量，形状为 [深度 - 1]
    price_changes = np.diff(prices)
    volume_changes = np.diff(volumes)

    # 获取原始的价格和成交量，去掉最后一个深度的价格和成交量，形状为 [深度 - 1]
    P = prices[:-1]
    V = volumes[:-1]

    # 计算弹性 (volume_changes / V) / (price_changes / P)，处理除零的情况
    with np.errstate(divide='ignore', invalid='ignore'):
        elasticity = (volume_changes / V) / (price_changes / P)

    # 去除 NaN 和 Inf 的弹性值
    valid_mask = ~np.isnan(elasticity) & ~np.isinf(elasticity)
    elasticity[~valid_mask] = np.nan

    # 计算平均弹性，忽略 NaN
    average_elasticity = np.nanmean(elasticity)
    return average_elasticity
