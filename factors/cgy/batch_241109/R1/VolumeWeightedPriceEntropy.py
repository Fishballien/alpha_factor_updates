import numpy as np

def VolumeWeightedPriceEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]
    total_volume = np.sum(volumes)
    if total_volume == 0:
        return 0
    volume_probs = volumes / total_volume
    weighted_price = np.sum(prices * volume_probs)
    price_diffs = prices - weighted_price
    entropy = -np.sum(volume_probs * np.log(np.abs(price_diffs) + 1e-9))
    return entropy
