import numpy as np

def VolumeWeightedMedianPrice(lob: np.array) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 检查总成交量是否为零
    total_volume = np.sum(volumes)
    if total_volume == 0:
        return np.nan

    # 对价格和成交量进行排序
    sorted_indices = np.argsort(prices)
    sorted_prices = prices[sorted_indices]
    sorted_volumes = volumes[sorted_indices]

    # 计算累积成交量
    cumulative_volumes = np.cumsum(sorted_volumes)

    # 找到中位数位置
    median_idx = np.searchsorted(cumulative_volumes, total_volume / 2)

    # 返回加权中位数价格
    return sorted_prices[median_idx]
