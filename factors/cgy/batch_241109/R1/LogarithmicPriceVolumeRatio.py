import numpy as np

def LogarithmicPriceVolumeRatio(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 计算价格与成交量的比例并取对数
    log_pv_ratio = np.log(np.sum(volumes) / np.sum(prices))

    return log_pv_ratio
