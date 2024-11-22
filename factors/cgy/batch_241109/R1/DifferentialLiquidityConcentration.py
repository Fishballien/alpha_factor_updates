import numpy as np

def DifferentialLiquidityConcentration(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价
    best_price = prices[0]

    # 计算流动性集中度（包含全部深度）
    full_concentration = np.sum(volumes * (prices - best_price)) / np.sum(volumes)

    # 计算流动性集中度（去掉最后一层深度）
    partial_concentration = np.sum(volumes[:-1] * (prices[:-1] - best_price)) / np.sum(volumes[:-1])

    # 计算差分
    concentration_diff = full_concentration - partial_concentration

    return concentration_diff
