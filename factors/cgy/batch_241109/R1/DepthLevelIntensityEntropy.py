import numpy as np

def DepthLevelIntensityEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    mean_volume = np.mean(volumes)
    intensity_levels = volumes / (mean_volume + 1e-9)
    total_intensity = np.sum(intensity_levels)
    if total_intensity == 0:
        return 0
    intensity_probs = intensity_levels / total_intensity
    entropy = -np.sum(intensity_probs * np.log(intensity_probs + 1e-9))
    return entropy
