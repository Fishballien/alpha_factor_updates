import numpy as np

def ConvexityMeasure(lob: np.array) -> float:
    # 获取最优价和最远价
    best_price = lob[0, 0]
    farthest_price = lob[-1, 0]

    # 计算价格均值
    price_mean = (best_price + farthest_price) / 2

    # 初始化凸性度量累积和
    convexity_sum = 0.0

    # 遍历每个深度（从第二个深度到倒数第二个深度）
    for i in range(1, lob.shape[0] - 1):
        # 前一个深度价格
        prev_price = lob[i - 1, 0]
        # 当前深度价格
        current_price = lob[i, 0]
        # 下一个深度价格
        next_price = lob[i + 1, 0]

        # 计算凸性公式中的分子部分
        convexity_value = abs((prev_price + next_price - 2 * current_price) / price_mean)

        # 累加凸性度量
        convexity_sum += convexity_value

    return convexity_sum
