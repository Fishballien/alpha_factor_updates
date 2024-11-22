import numpy as np

def PriceMomentumSkew(lob: np.array) -> float:
    # 提取价格数据
    prices = lob[:, 0]

    # 计算最优价格
    P_best = prices[0]

    # 计算深度范围的均价和标准差
    P_depth_mean = np.mean(prices)
    P_depth_std = np.std(prices)

    # 避免除以 0 的情况
    if P_depth_std == 0:
        return np.nan

    # 计算价格动量偏度
    price_momentum_skew = (P_depth_mean - P_best) / P_depth_std
    return price_momentum_skew
