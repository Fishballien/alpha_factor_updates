import numpy as np

def VolumeDispersion(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 计算总成交量
    V_total = volumes.sum()

    # 计算成交量比例
    volume_ratios = volumes / V_total

    # 计算均匀比例
    N = volumes.shape[0]  # 深度的大小
    uniform_ratio = 1 / N

    # 计算分散度
    dispersion = np.sqrt(np.sum((volume_ratios - uniform_ratio) ** 2) / N)

    return dispersion
