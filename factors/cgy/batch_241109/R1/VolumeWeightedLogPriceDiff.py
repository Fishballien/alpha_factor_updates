import numpy as np

def VolumeWeightedLogPriceDiff(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]
    
    # 最优价
    best_price = prices[0]
    
    # 计算每个价位的对数相对价格差异
    log_price_diff = np.log(prices / best_price)
    
    # 计算成交量加权的对数价格差异
    vwld = np.sum(volumes * log_price_diff)

    return vwld
