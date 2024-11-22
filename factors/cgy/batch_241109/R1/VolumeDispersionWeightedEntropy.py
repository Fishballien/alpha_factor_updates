import numpy as np

def VolumeDispersionWeightedEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    mean_volume = np.mean(volumes)
    dispersion_weights = np.abs(volumes - mean_volume)
    total_dispersion = np.sum(dispersion_weights)
    if total_dispersion == 0:
        return 0
    dispersion_probs = dispersion_weights / total_dispersion
    entropy = -np.sum(dispersion_probs * np.log(dispersion_probs + 1e-9))
    return entropy
