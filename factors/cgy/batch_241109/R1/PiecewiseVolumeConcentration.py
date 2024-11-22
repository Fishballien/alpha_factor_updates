import numpy as np

def PiecewiseVolumeConcentration(lob: np.array) -> float:
    # 提取价格和成交量，[深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 中间价位
    mid_price = prices[len(prices) // 2]

    # 计算低于和高于中间价位的流动性集中度
    lower_concentration = np.sum(volumes[prices <= mid_price])
    upper_concentration = np.sum(volumes[prices > mid_price])

    # 返回根据最优价选择的流动性集中度
    return lower_concentration if prices[0] <= mid_price else upper_concentration
