import numpy as np

def SlopeChangeRate(lob: np.array) -> float:
    # 检查是否有足够的深度进行计算
    depth = lob.shape[0]
    if depth < 2:
        return np.nan

    # 提取最优价
    P_best = lob[0, 0]

    # 计算深度 d 处的斜率
    slope_d = (lob[depth - 1, 0] - P_best) / depth

    # 计算深度 d-1 处的斜率
    slope_d_minus_1 = (lob[depth - 2, 0] - P_best) / (depth - 1)

    # 计算斜率变化率
    slope_change_rate = abs(slope_d - slope_d_minus_1)

    return slope_change_rate
