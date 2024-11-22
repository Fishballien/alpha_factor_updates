import numpy as np

def SingularValueSpread(lob: np.array) -> float:
    # 提取价格和成交量矩阵
    ask_matrix = lob[:, 0:2]

    # 对 ask_matrix 进行奇异值分解，返回前两个奇异值
    singular_values = np.linalg.svd(ask_matrix, compute_uv=False)

    # 计算第一大奇异值与第二大奇异值之差
    singular_value_spread = singular_values[0] - singular_values[1]

    return singular_value_spread


