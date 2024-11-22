import numpy as np

def SoftMaxVolumeConcentration(lob: np.array) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价
    best_price = prices[0]

    # 计算 SoftMax 权重
    softmax_weights = np.exp(prices - best_price) / np.sum(np.exp(prices - best_price))

    # 计算加权成交量
    softmax_concentration = np.sum(softmax_weights * volumes)

    return softmax_concentration
