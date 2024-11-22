import numpy as np

def PriceSkewness(lob: np.array) -> float:
    skewness_sum = 0.0
    weight_sum = 0.0

    for i in range(1, lob.shape[0] - 1):
        price_prev = lob[i - 1, 0]
        price_curr = lob[i, 0]
        price_next = lob[i + 1, 0]

        volume = lob[i, 1]

        skewness_term = (price_prev - price_curr)**3 + (price_next - price_curr)**3

        skewness_sum += skewness_term * volume
        weight_sum += volume

    return skewness_sum / weight_sum if weight_sum != 0 else np.nan