import numpy as np

def CumulativeVolumeEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    cumulative_volumes = np.cumsum(volumes)
    total_cumulative_volume = cumulative_volumes[-1]
    if total_cumulative_volume == 0:
        return 0
    volume_probs = cumulative_volumes / total_cumulative_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
