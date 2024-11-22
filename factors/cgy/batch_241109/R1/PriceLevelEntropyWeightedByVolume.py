import numpy as np

def PriceLevelEntropyWeightedByVolume(lob: np.array) -> float:
    # 提取成交量
    volumes = lob[:, 1]

    # 计算总成交量
    V_total = np.sum(volumes)

    # 避免总成交量为 0 的情况
    if V_total == 0:
        return np.nan

    # 初始化加权熵值
    weighted_entropy = 0.0

    # 计算每个深度的概率及其权重，逐项遍历
    for i in range(lob.shape[0]):
        probability = volumes[i] / V_total
        weight = i + 1  # 深度从1开始计数

        # 跳过零概率项以避免 log(0)
        if probability > 0:
            weighted_probability = weight * probability
            weighted_entropy -= weighted_probability * np.log(weighted_probability)

    # 归一化加权概率以确保和为1（如有需要）
    weighted_entropy /= np.sum(np.arange(1, lob.shape[0] + 1))

    return weighted_entropy
