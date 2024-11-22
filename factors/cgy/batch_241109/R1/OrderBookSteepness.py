import numpy as np

def OrderBookSteepness(lob: np.array) -> float:
    # 获取价格层数
    N = lob.shape[0]

    # 计算陡峭度: (最后一个价格 - 第一个价格) / (价格层数 - 1)
    first_price = lob[0, 0]  # 最优价（第一个价格）
    last_price = lob[-1, 0]  # 最深层的价格

    steepness = (last_price - first_price) / (N - 1)

    return steepness
