import numpy as np

def MaxVolumePriceLevel(lob: np.array) -> float:
    # 初始化最大成交量和对应价格
    max_volume = 0
    price_max_volume = np.nan

    # 遍历每个深度，寻找最大挂单量
    for i in range(lob.shape[0]):
        current_price = lob[i, 0]
        current_volume = lob[i, 1]

        if current_volume > max_volume:
            max_volume = current_volume
            price_max_volume = current_price

    return price_max_volume
