import numpy as np

def PriceLevelHarmonicOscillation(lob: np.array) -> float:
    # 检查深度是否足够计算价格振荡
    depth_len = lob.shape[0]
    if depth_len < 3:
        return np.nan

    # 初始化价格振荡累积和
    total_oscillation = 0.0

    # 逐深度计算价格振荡
    for i in range(1, depth_len - 1):
        price_prev = lob[i - 1, 0]
        price_curr = lob[i, 0]
        price_next = lob[i + 1, 0]

        # 计算当前深度的价格振荡：|price_prev - 2 * price_curr + price_next|
        oscillation = abs(price_prev - 2 * price_curr + price_next)

        # 累加到总振荡中
        total_oscillation += oscillation

    return total_oscillation
