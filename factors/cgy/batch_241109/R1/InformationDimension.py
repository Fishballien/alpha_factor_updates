import numpy as np

def InformationDimension(lob: np.array, epsilon=0.1) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 计算总成交量
    total_volume = volumes.sum()
    if total_volume == 0:
        return np.nan

    # 计算各深度的概率，去除为零的概率
    probabilities = volumes / total_volume
    probabilities = probabilities[probabilities > 0]

    # 计算熵
    entropy = -np.sum(probabilities * np.log(probabilities))

    # 计算信息维度 D1
    D1 = entropy / np.log(1 / epsilon)

    return D1
