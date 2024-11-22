import numpy as np

def CumulativeVolumePercentilePrice(lob: np.array, percentile: float = 0.5) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算总成交量
    V_total = np.sum(volumes)

    # 检查总成交量是否足够
    if V_total == 0:
        return np.nan

    # 计算目标成交量
    target_volume = percentile * V_total

    # 计算累计成交量
    cumulative_volume = 0.0
    for i in range(len(volumes)):
        cumulative_volume += volumes[i]

        # 检查是否达到目标成交量
        if cumulative_volume >= target_volume:
            return prices[i]

    # 如果未达到目标成交量，返回 NaN
    return np.nan
