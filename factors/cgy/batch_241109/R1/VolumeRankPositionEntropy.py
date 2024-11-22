import numpy as np

def VolumeRankPositionEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    sorted_volumes = np.sort(volumes)[::-1]
    rank_weights = np.arange(1, len(sorted_volumes) + 1)
    weighted_volumes = sorted_volumes * rank_weights
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
