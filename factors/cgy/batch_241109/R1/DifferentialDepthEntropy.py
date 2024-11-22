import numpy as np

def DifferentialDepthEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    volume_diffs = np.abs(np.diff(volumes))
    total_diff = np.sum(volume_diffs)
    if total_diff == 0:
        return 0
    diff_probs = volume_diffs / total_diff
    entropy = -np.sum(diff_probs * np.log(diff_probs + 1e-9))
    return entropy
