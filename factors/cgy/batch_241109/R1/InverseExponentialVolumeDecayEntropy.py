import numpy as np

def InverseExponentialVolumeDecayEntropy(lob: np.array, decay_rate: float = 0.05) -> float:
    volumes = lob[:, 1]
    decay_weights = 1 / (np.exp(decay_rate * np.arange(len(volumes))) + 1e-9)
    weighted_volumes = volumes * decay_weights
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
