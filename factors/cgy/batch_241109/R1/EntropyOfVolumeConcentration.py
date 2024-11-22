import numpy as np

def EntropyOfVolumeConcentration(lob: np.array) -> float:
    volumes = lob[:, 1]
    total_volume = np.sum(volumes)
    if total_volume == 0:
        return 0
    volume_ratios = volumes / total_volume
    entropy = -np.sum(volume_ratios * np.log(volume_ratios + 1e-9))
    return entropy
