import numpy as np

def InverseVolumeGapEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    inverse_volumes = 1 / (volumes + 1e-9)  # 避免除以零
    total_inverse_volume = np.sum(inverse_volumes)
    if total_inverse_volume == 0:
        return 0
    volume_probs = inverse_volumes / total_inverse_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
