import numpy as np

def MutualInformation(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]
    
    # 计算价格和成交量的均值
    mean_price = np.mean(prices)
    mean_volume = np.mean(volumes)
    
    # 计算互信息
    with np.errstate(divide='ignore', invalid='ignore'):
        mi_price = np.nansum(prices * np.log(prices / (mean_price + 1e-9)))
        mi_volume = np.nansum(volumes * np.log(volumes / (mean_volume + 1e-9)))
    
    mutual_information = mi_price + mi_volume
    
    return mutual_information
