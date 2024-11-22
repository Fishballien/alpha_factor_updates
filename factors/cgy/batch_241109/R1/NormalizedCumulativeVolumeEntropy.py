import numpy as np

def NormalizedCumulativeVolumeEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    cumulative_volumes = np.cumsum(volumes) / (np.sum(volumes) + 1e-9)
    entropy = -np.sum(cumulative_volumes * np.log(cumulative_volumes + 1e-9))
    return entropy
