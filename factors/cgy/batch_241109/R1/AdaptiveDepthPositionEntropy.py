import numpy as np

def AdaptiveDepthPositionEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    depth_positions = np.arange(1, len(volumes) + 1) / len(volumes)
    weighted_volumes = volumes * depth_positions
    total_weighted_volume = np.sum(weighted_volumes)
    if total_weighted_volume == 0:
        return 0
    volume_probs = weighted_volumes / total_weighted_volume
    entropy = -np.sum(volume_probs * np.log(volume_probs + 1e-9))
    return entropy
