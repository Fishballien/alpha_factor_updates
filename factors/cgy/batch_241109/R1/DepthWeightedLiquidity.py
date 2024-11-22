import numpy as np

def DepthWeightedLiquidity(lob: np.array) -> float:
    # 提取最优价
    P_best = lob[0, 0]

    # 初始化深度加权流动性累加和
    depth_weighted_liquidity = 0.0

    # 遍历每个深度位置
    for i in range(lob.shape[0]):
        price = lob[i, 0]
        volume = lob[i, 1]
        
        # 计算价格差
        price_diff = price - P_best
        
        # 跳过价格差为0的情况以避免除零错误
        if price_diff != 0:
            depth_weighted_liquidity += volume / price_diff

    return depth_weighted_liquidity if depth_weighted_liquidity != 0 else np.nan
