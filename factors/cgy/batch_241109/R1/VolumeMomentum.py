import numpy as np

def VolumeMomentum(lob: np.array) -> float:
    # 初始化成交量动量累积和
    volume_momentum_sum = 0.0

    # 遍历每个深度（从第二个深度开始）计算成交量变化
    for i in range(1, lob.shape[0]):
        # 当前和前一个深度的成交量
        current_volume = lob[i, 1]
        previous_volume = lob[i - 1, 1]

        # 计算当前深度的成交量变化
        volume_change = current_volume - previous_volume

        # 累加成交量变化
        volume_momentum_sum += volume_change

    return volume_momentum_sum
