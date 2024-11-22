import numpy as np

def AdaptivePriceSensitivity(lob: np.array) -> float:
    # 初始化价格敏感性累积和
    sensitivity_sum = 0.0

    # 遍历每个深度（从第二个深度开始）计算价格变化比率和加权敏感性
    for i in range(1, lob.shape[0]):
        # 当前价格、成交量和前一个价格
        current_price = lob[i, 0]
        previous_price = lob[i - 1, 0]
        current_volume = lob[i, 1]

        # 计算价格变化比率的绝对值
        if previous_price != 0:
            price_diff_ratio = abs(current_price - previous_price) / previous_price
        else:
            price_diff_ratio = 0

        # 计算并累加加权的价格敏感性
        sensitivity_sum += price_diff_ratio * current_volume

    return sensitivity_sum
