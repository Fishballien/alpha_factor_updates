import numpy as np

def RootMeanSquareDepth(lob: np.array) -> float:
    prices = lob[:, 0]
        
    # 最优价，即第一层价格
    best_price = prices[0]
    
    # 计算均方根深度
    rms_depth = np.sqrt(np.mean((prices - best_price) ** 2))
    
    return rms_depth
