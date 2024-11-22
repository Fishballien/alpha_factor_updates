import numpy as np

def VolumeDecayRate(lob: np.array) -> float:
    # 检查深度是否足够
    N = lob.shape[0]
    if N < 2 or lob[0, 1] <= 0 or lob[-1, 1] <= 0:
        return np.nan

    # 提取起始和终止位置的成交量
    initial_volume = lob[0, 1]
    final_volume = lob[-1, 1]

    # 计算衰减率
    decay_rate = -np.log(final_volume / initial_volume) / (N - 1)

    return decay_rate if initial_volume > 0 else np.nan
