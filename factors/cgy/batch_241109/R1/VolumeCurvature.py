import numpy as np

def VolumeCurvature(lob: np.array) -> float:
    # 初始化曲率累积和
    curvature_sum = 0.0

    # 遍历从第 2 到倒数第 2 个深度（以计算二阶差分）
    for i in range(1, lob.shape[0] - 1):
        # 计算前后深度的价格差分
        price_diff = lob[i + 1, 0] - lob[i - 1, 0]

        # 计算当前深度的二阶成交量差分
        volume_diff2 = lob[i + 1, 1] - 2 * lob[i, 1] + lob[i - 1, 1]

        # 计算曲率，避免除以零
        curvature = abs(volume_diff2) / (price_diff ** 2 + 1e-8)

        # 累加当前深度的曲率
        curvature_sum += curvature

    return curvature_sum
