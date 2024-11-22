import numpy as np

def DepthEntropyWithGaussianDecay(lob: np.array, sigma: float = 0.5) -> float:
    volumes = lob[:, 1]
    depth_indices = np.arange(len(volumes))
    gaussian_weights = np.exp(-((depth_indices - len(volumes) / 2) ** 2) / (2 * sigma ** 2))
    weighted_volumes = volumes * gaussian_weights
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
