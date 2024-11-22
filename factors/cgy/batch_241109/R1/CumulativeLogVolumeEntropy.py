import numpy as np

def CumulativeLogVolumeEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    cumulative_log_volumes = np.cumsum(np.log1p(volumes))
    normalized_cumulative = cumulative_log_volumes / (cumulative_log_volumes[-1] + 1e-9)
    entropy = -np.sum(normalized_cumulative * np.log(normalized_cumulative + 1e-9))
    return entropy
