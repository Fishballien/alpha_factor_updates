import numpy as np

def VolumeConvexity(lob: np.array) -> float:
    convexity_sum = 0.0
    weight_sum = 0.0

    for i in range(1, lob.shape[0] - 1):
        volume_prev = lob[i - 1, 1]
        volume_curr = lob[i, 1]
        volume_next = lob[i + 1, 1]

        volume = lob[i, 1]

        convexity_term = volume_prev - 2 * volume_curr + volume_next

        convexity_sum += convexity_term * volume
        weight_sum += volume

    return convexity_sum / weight_sum if weight_sum != 0 else np.nan