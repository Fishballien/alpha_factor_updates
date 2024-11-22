import numpy as np

def AdaptiveLiquidityDensity(lob: np.array, volume_threshold_ratio: float = 0.1) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]
    V_total = np.sum(volumes)

    # 计算目标成交量
    target_volume = volume_threshold_ratio * V_total

    # 初始化累积成交量
    cumulative_volume = 0.0

    # 初始化索引和有效变量
    V_effective = 0.0
    P_effective = 0.0

    # 获取最优价
    P_best = prices[0]

    # 遍历深度，找到满足目标成交量的第一个位置
    for i in range(lob.shape[0]):
        cumulative_volume += volumes[i]

        if cumulative_volume >= target_volume:
            V_effective = cumulative_volume
            P_effective = prices[i]
            break

    # 如果未找到满足条件的位置，使用最大深度的价格和累积量
    if V_effective == 0.0:
        V_effective = V_total
        P_effective = prices[-1]

    # 计算价格差并避免除以零
    price_diff = P_effective - P_best
    if price_diff == 0:
        return np.nan

    # 计算并返回流动性密度
    liquidity_density = V_effective / price_diff
    return liquidity_density
