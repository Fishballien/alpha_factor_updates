import numpy as np

def LogarithmicPriceImpact(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]

    # 最优价
    best_price = prices[0]

    # 计算累积成交量和达到50%总量的价格水平
    cumulative_volumes = np.cumsum(volumes)
    total_volumes = np.sum(volumes)
    price_effective_idx = np.argmax(cumulative_volumes >= 0.5 * total_volumes)
    price_effective = prices[price_effective_idx]

    # 计算价格影响因子
    price_impact = np.log(price_effective / best_price) * total_volumes

    return price_impact
