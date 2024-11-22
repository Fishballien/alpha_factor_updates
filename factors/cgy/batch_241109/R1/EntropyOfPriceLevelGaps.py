import numpy as np

def EntropyOfPriceLevelGaps(lob: np.array) -> float:
    # 提取价格，形状为 [深度]
    prices = lob[:, 0]

    # 计算价格层次的间隔，形状为 [深度 - 1]
    price_gaps = np.round(np.diff(prices), 12)
    price_gaps = price_gaps[price_gaps>0]

    # 计算总间隔
    total_gaps = price_gaps.sum()

    if total_gaps == 0:
        return 0  # 如果总间隔为0，则熵为0

    # 计算概率
    probabilities = price_gaps / total_gaps
    if any(probabilities<0):
        print(price_gaps, total_gaps, lob)

    # 计算熵
    entropy = -np.sum(probabilities * np.log(probabilities + 1e-9))  # 防止 log(0)

    return entropy
