import numpy as np

def VolumeConcentrationIndex(lob: np.array) -> float:
    # 提取成交量
    volumes = lob[:, 1]

    # 计算总成交量
    V_total = np.sum(volumes)

    # 如果总成交量为零，返回 NaN
    if V_total == 0:
        return np.nan

    # 计算每个深度位置的成交量比例并计算其平方和
    concentration_index = 0.0
    for volume in volumes:
        volume_ratio = volume / V_total
        concentration_index += volume_ratio ** 2

    return concentration_index
