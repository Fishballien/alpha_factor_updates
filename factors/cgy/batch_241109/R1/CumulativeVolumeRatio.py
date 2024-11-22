import numpy as np

def CumulativeVolumeRatio(lob: np.array) -> float:
    # 初始化累积成交量
    V_cumulative_d1 = 0.0
    V_cumulative_d2 = 0.0

    # 遍历深度计算累积成交量
    for i in range(lob.shape[0]):
        current_volume = lob[i, 1]

        if i == 1:
            V_cumulative_d1 = np.sum(lob[:2, 1])  # 前两个深度的成交量累积
        elif i == 2:
            V_cumulative_d2 = np.sum(lob[:3, 1])  # 前三个深度的成交量累积
            break

    # 计算累积成交量比率
    cumulative_volume_ratio = V_cumulative_d2 / V_cumulative_d1 if V_cumulative_d1 != 0 else np.nan

    return cumulative_volume_ratio
