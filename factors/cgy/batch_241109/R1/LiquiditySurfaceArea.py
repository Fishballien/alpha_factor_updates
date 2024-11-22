import numpy as np

def LiquiditySurfaceArea(lob: np.array) -> float:
    # 初始化流动性表面积累加器
    surface_area_sum = 0.0

    # 遍历每个深度，计算价格差和平均成交量
    for i in range(1, lob.shape[0]):
        # 当前深度和前一个深度的价格与成交量
        price_current = lob[i, 0]
        price_previous = lob[i - 1, 0]
        volume_current = lob[i, 1]
        volume_previous = lob[i - 1, 1]

        # 计算价格差异
        price_difference = price_current - price_previous

        # 计算平均成交量
        average_volume = (volume_current + volume_previous) / 2

        # 累加表面积（价格差异 * 平均成交量）
        surface_area_sum += price_difference * average_volume

    return surface_area_sum
