import numpy as np

def PriceLevelRankEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]
    rank_weights = np.arange(1, len(prices) + 1)
    total_rank = np.sum(rank_weights)
    if total_rank == 0:
        return 0
    rank_probs = rank_weights / total_rank
    entropy = -np.sum(rank_probs * np.log(rank_probs + 1e-9))
    return entropy
