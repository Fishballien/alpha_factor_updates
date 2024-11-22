import numpy as np

def KurtosisDepth(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算加权均价
    weighted_mean_price = np.sum(prices * volumes) / np.sum(volumes)

    # 计算加权标准差
    price_diff = prices - weighted_mean_price
    weighted_std = np.sqrt(np.sum((price_diff ** 2) * volumes) / np.sum(volumes))

    # 计算峰度（为了防止除以零，添加一个小的常数）
    kurtosis = np.sum((price_diff ** 4) * volumes) / ((len(prices) - 1) * (weighted_std ** 4) + 1e-9)

    return kurtosis
