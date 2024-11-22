import numpy as np
from numpy.fft import fft

def FourierDepth(lob: np.array) -> float:
    # 提取成交量，形状为 [深度]
    volumes = lob[:, 1]

    # 计算傅里叶变换，形状为 [深度]
    fft_result = fft(volumes)

    # 提取主要频率成分（忽略直流分量）
    dominant_frequency = np.abs(fft_result[1])

    return dominant_frequency
