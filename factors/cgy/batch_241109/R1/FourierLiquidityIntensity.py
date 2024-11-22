import numpy as np

def FourierLiquidityIntensity(lob: np.array) -> float:
    # 提取价格和成交量，形状为 [深度]
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 设置傅里叶参数
    omega = np.pi / 5  # 频率
    phi = np.pi / 4    # 相位偏移

    # 计算傅里叶流动性强度
    fli = np.sum(volumes * np.sin(omega * prices + phi))

    return fli

