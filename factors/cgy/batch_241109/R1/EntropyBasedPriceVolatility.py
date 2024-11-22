import numpy as np

def EntropyBasedPriceVolatility(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]
    
    # 计算总成交量
    total_volume = np.sum(volumes)

    # 计算每个价位的成交量概率
    probabilities = volumes / total_volume

    # 避免取对数零的情况
    non_zero_probs = probabilities > 0

    # 计算熵
    entropy = -np.sum(probabilities[non_zero_probs] * np.log(probabilities[non_zero_probs]))

    return entropy
