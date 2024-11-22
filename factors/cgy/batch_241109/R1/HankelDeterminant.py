import numpy as np
from scipy.linalg import hankel

def HankelDeterminant(lob: np.array, max_values: int = 10) -> float:
    # hankel计算复杂度太高，因此间隔取值。
    # 动态计算 step，使得取到的值不超过 max_values
    step = max(1, lob.shape[0] // max_values)

    # 提取成交量并进行动态间隔取值
    volumes = lob[::step, 1]

    # 限制取值最多为 max_values 个
    if len(volumes) > max_values:
        volumes = volumes[:max_values]

    # 检查是否有足够数据构造 Hankel 矩阵
    if len(volumes) < 2:
        return np.nan

    # 构造 Hankel 矩阵
    hankel_matrix = hankel(volumes)

    # 计算 Hankel 矩阵的行列式
    determinant = np.linalg.det(hankel_matrix)

    return determinant
