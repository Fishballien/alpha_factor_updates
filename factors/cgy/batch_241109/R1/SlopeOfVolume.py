import numpy as np

def SlopeOfVolume(lob: np.array) -> float:
    # 提取第一个深度位置的成交量（最顶层）
    top_volume = lob[0, 1]

    # 提取最后一个深度位置的成交量（最底层）
    bottom_volume = lob[-1, 1]

    # 获取深度的数量
    depth = lob.shape[0]

    # 计算成交量的斜率
    slope = (bottom_volume - top_volume) / depth

    return slope
