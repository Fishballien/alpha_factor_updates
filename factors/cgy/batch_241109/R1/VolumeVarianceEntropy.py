import numpy as np

def VolumeVarianceEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    volume_variances = np.var(volumes)
    variance_probs = (volumes - volume_variances) ** 2 / np.sum((volumes - volume_variances) ** 2 + 1e-9)
    entropy = -np.sum(variance_probs * np.log(variance_probs + 1e-9))
    return entropy
