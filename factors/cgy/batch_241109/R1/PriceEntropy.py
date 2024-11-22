import numpy as np

def PriceEntropy(lob: np.array) -> float:
    # 提取成交量
    volumes = lob[:, 1]

    # 计算每个价位的成交量比例
    total_volume = np.sum(volumes)
    if total_volume == 0:
        return 0  # 如果总成交量为0，返回熵为0
    
    volume_proportion = volumes / total_volume

    # 计算熵
    entropy = -np.sum(volume_proportion * np.log(volume_proportion + 1e-9))  # 防止log(0)

    return entropy
