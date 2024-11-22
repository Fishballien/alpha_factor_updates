import numpy as np

def Concentration(lob: np.array) -> float:
    # 初始化成交量累加和与平方和
    volume_sum = 0.0
    volume_square_sum = 0.0

    # 遍历每个深度，计算成交量和成交量平方和
    for i in range(lob.shape[0]):
        volume = lob[i, 1]
        volume_sum += volume
        volume_square_sum += volume ** 2

    # 计算集中度指数
    concentration = volume_square_sum / (volume_sum ** 2) if volume_sum != 0 else np.nan

    return concentration
