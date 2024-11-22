import numpy as np

def CumulativeInverseVolumeEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    inverse_volumes = 1 / (volumes + 1e-9)
    cumulative_inverse = np.cumsum(inverse_volumes)
    total_cumulative = cumulative_inverse[-1]
    if total_cumulative == 0:
        return 0
    inverse_probs = cumulative_inverse / total_cumulative
    entropy = -np.sum(inverse_probs * np.log(inverse_probs + 1e-9))
    return entropy
