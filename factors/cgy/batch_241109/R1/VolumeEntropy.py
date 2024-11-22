import numpy as np

def VolumeEntropy(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 计算总成交量
    V_total = volumes.sum()

    if V_total == 0:
        return 0  # 如果总成交量为0，则熵为0

    # 计算成交量的概率分布
    probabilities = volumes / V_total

    # 计算熵，忽略零概率的对数
    entropy = -np.nansum(probabilities * np.log(probabilities, where=(probabilities > 0)))

    return entropy
