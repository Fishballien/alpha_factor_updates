import numpy as np

def AnisotropyIndex(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 获取深度的数量 N
    N = volumes.shape[0]

    # 计算每个深度位置的角度，形状为 [深度]
    angles = 2 * np.pi * np.arange(N) / N

    # 计算 x 和 y 分量
    x = np.sum(volumes * np.cos(angles))
    y = np.sum(volumes * np.sin(angles))

    # 计算总成交量
    V_total = np.sum(volumes)

    # 计算异向性指数
    anisotropy_index = (x ** 2 + y ** 2) / V_total ** 2 if V_total != 0 else np.nan

    return anisotropy_index