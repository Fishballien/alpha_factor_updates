import numpy as np

def PressureIndex(lob: np.array) -> float:
    # 最优价
    P_best = lob[0, 0]

    # 计算平均价格
    P_ask_mean = np.mean(lob[:, 0])

    # 计算价格的标准差
    P_std = np.std(lob[:, 0])

    # 避免除以零的情况
    if P_std == 0:
        return np.nan

    # 计算卖单压力指数
    ask_pressure_index = (P_best - P_ask_mean) / P_std

    return ask_pressure_index
