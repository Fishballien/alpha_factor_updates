import numpy as np

def DepthVolatility(lob: np.array) -> float:
    # 提取成交量数据
    volumes = lob[:, 1]

    # 计算均值
    total_volume = 0.0
    for volume in volumes:
        total_volume += volume
    mean_volume = total_volume / len(volumes)

    # 计算方差
    variance_sum = 0.0
    for volume in volumes:
        variance_sum += (volume - mean_volume) ** 2
    variance = variance_sum / (len(volumes) - 1)

    # 计算标准差（波动率）
    volatility = np.sqrt(variance)

    return volatility
