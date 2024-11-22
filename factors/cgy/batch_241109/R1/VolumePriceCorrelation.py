import numpy as np

def VolumePriceCorrelation(lob: np.array) -> float:
    # 提取价格和成交量
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算价格和成交量的均值
    mean_price = np.mean(prices)
    mean_volume = np.mean(volumes)

    # 计算协方差的分子部分
    covariance_numerator = 0.0
    for i in range(lob.shape[0]):
        covariance_numerator += (prices[i] - mean_price) * (volumes[i] - mean_volume)

    # 计算价格和成交量的标准差
    std_price = np.sqrt(np.sum((prices - mean_price) ** 2))
    std_volume = np.sqrt(np.sum((volumes - mean_volume) ** 2))

    # 计算相关系数，如果标准差为0，则返回NaN
    if std_price == 0 or std_volume == 0:
        return np.nan

    correlation = covariance_numerator / (std_price * std_volume)

    return correlation
