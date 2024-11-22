import numpy as np

def EigenLiquidity(lob: np.array) -> float:
    """Calculate the maximum eigenvalue of the covariance matrix formed by prices and volumes.

    Args:
        lob_ask (np.array): Shape [深度, 2], 包含一个时间点的 LOB 数据, 2表示[price, volume]。

    Returns:
        float: 协方差矩阵的最大特征值（流动性指标）。
    """
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 构造价格-成交量矩阵，形状为 [2, 深度]
    price_volume_matrix = np.vstack((prices, volumes))

    # 计算价格-成交量的协方差矩阵
    covariance_matrix = np.cov(price_volume_matrix)

    # 计算协方差矩阵的最大特征值
    max_eigenvalue = np.max(np.linalg.eigvals(covariance_matrix))

    return max_eigenvalue
