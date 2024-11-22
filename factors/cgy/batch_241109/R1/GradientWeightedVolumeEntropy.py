import numpy as np

def GradientWeightedVolumeEntropy(lob: np.array) -> float:
    prices = lob[:, 0]
    volumes = lob[:, 1]
    price_gradients = np.abs(np.diff(prices))
    weighted_volumes = volumes[1:] * price_gradients
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
