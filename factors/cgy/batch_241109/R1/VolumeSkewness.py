import numpy as np

def VolumeSkewness(lob: np.array) -> float:
    skewness_sum = 0.0
    weight_sum = 0.0

    for i in range(1, lob.shape[0] - 1):
        volume_prev = lob[i - 1, 1]
        volume_curr = lob[i, 1]
        volume_next = lob[i + 1, 1]

        volume = lob[i, 1]

        skewness_term = (volume_prev - volume_curr)**3 + (volume_next - volume_curr)**3

        skewness_sum += skewness_term * volume
        weight_sum += volume

    return skewness_sum / weight_sum if weight_sum != 0 else np.nan