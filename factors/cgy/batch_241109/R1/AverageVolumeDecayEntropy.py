import numpy as np

def AverageVolumeDecayEntropy(lob: np.array, decay: float = 0.05) -> float:
    volumes = lob[:, 1]
    decay_weights = np.exp(-decay * np.arange(len(volumes)))
    weighted_volumes = volumes * decay_weights
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
