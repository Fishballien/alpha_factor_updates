import numpy as np

def PriceLevelClusteringCoefficient(lob: np.array, volume_threshold_ratio=0.05) -> float:
    # 计算总成交量
    V_total = np.sum(lob[:, 1])

    # 计算高成交量阈值
    threshold = volume_threshold_ratio * V_total

    # 初始化集群计数器
    num_clusters = 0
    in_cluster = False

    # 遍历每个深度，检查是否超过阈值
    for i in range(lob.shape[0]):
        current_volume = lob[i, 1]

        # 判断是否为高成交量位置
        if current_volume > threshold:
            if not in_cluster:
                # 开始一个新的集群
                num_clusters += 1
                in_cluster = True
        else:
            in_cluster = False  # 结束当前集群

    # 计算聚类系数
    N = lob.shape[0]  # 深度的总数
    clustering_coefficient = num_clusters / N if N != 0 else np.nan

    return clustering_coefficient
