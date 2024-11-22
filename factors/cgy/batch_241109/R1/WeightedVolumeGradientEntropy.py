import numpy as np

def WeightedVolumeGradientEntropy(lob: np.array) -> float:
    volumes = lob[:, 1]
    volume_gradients = np.abs(np.diff(volumes))
    total_gradient = np.sum(volume_gradients)
    if total_gradient == 0:
        return 0
    gradient_probs = volume_gradients / total_gradient
    entropy = -np.sum(gradient_probs * np.log(gradient_probs + 1e-9))
    return entropy
