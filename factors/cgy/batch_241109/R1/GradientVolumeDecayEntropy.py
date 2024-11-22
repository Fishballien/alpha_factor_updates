import numpy as np

def GradientVolumeDecayEntropy(lob: np.array, decay_rate: float = 0.1) -> float:
    volumes = lob[:, 1]
    volume_gradient = np.abs(np.diff(volumes))
    decay_weights = np.exp(-decay_rate * np.arange(len(volume_gradient)))
    weighted_gradient = volume_gradient * decay_weights
    total_weighted_gradient = np.sum(weighted_gradient)
    if total_weighted_gradient == 0:
        return 0
    gradient_probs = weighted_gradient / total_weighted_gradient
    entropy = -np.sum(gradient_probs * np.log(gradient_probs + 1e-9))
    return entropy
