import numpy as np

def LiquidityImbalanceIndex(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 获取总深度 N
    N = volumes.shape[0]
    if N < 2:
        return np.nan

    # 计算总成交量
    V_total = volumes.sum()

    # 计算上半部分和下半部分的成交量
    half_N = N // 2
    upper_volume_sum = volumes[:half_N].sum()
    lower_volume_sum = volumes[half_N:].sum()

    # 计算流动性失衡指数
    imbalance = (upper_volume_sum - lower_volume_sum) / V_total

    return imbalance
