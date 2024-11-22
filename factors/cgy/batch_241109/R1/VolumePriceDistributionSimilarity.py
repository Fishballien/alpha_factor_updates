import numpy as np

def VolumePriceDistributionSimilarity(lob: np.array) -> float:
    # 提取成交量和价格
    volumes = lob[:, 1]
    prices = lob[:, 0]

    # 计算总成交量和总价格
    V_total = volumes.sum()
    P_total = prices.sum()

    # 计算成交量和价格的比例
    volume_ratios = volumes / V_total
    price_ratios = prices / P_total

    # 计算分布相似度，使用 np.minimum 来获得每个深度的最小值并累加
    similarity = np.sum(np.minimum(volume_ratios, price_ratios))

    return similarity
