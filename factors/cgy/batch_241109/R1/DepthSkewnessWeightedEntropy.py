import numpy as np

def DepthSkewnessWeightedEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    depth_indices = np.arange(len(volumes))
    skewness_weights = (depth_indices - np.mean(depth_indices)) ** 3
    weighted_volumes = volumes * np.abs(skewness_weights)
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
