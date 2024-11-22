import numpy as np

def VolumeRankEntropy(lob: np.array) -> float:
    volumes = np.sort(lob[:, 1])[::-1]
    total_volume = np.sum(volumes)
    if total_volume == 0:
        return 0
    volume_probs = volumes / total_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
