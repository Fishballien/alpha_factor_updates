import numpy as np

def LogarithmicVolumeEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    log_volumes = np.log1p(volumes)
    total_log_volume = np.sum(log_volumes)
    if total_log_volume == 0:
        return 0
    volume_probs = log_volumes / total_log_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
