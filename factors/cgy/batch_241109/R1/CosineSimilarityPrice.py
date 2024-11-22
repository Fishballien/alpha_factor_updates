import numpy as np

def CosineSimilarityPrice(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价 (取最小的价格)
    best_price = prices[0]

    # 计算每个价位的偏差和平均挂单量
    price_diff = prices - best_price
    mean_volume = np.mean(volumes)
    volume_diff = volumes - mean_volume

    # 计算余弦相似度
    numerator = np.sum(price_diff * volume_diff)
    denominator = np.sqrt(np.sum(price_diff ** 2)) * np.sqrt(np.sum(volume_diff ** 2))
    cosine_similarity = numerator / (denominator + 1e-9)  # 防止除零错误

    return cosine_similarity
