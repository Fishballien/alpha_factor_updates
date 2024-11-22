import numpy as np

def PriceKurtosis(lob: np.array) -> float:
    kurtosis_sum = 0.0
    weight_sum = 0.0

    for i in range(1, lob.shape[0] - 1):
        price_prev = lob[i - 1, 0]
        price_curr = lob[i, 0]
        price_next = lob[i + 1, 0]

        volume = lob[i, 1]

        kurtosis_term = (price_prev - price_curr)**4 + (price_next - price_curr)**4

        kurtosis_sum += kurtosis_term * volume
        weight_sum += volume

    return kurtosis_sum / weight_sum if weight_sum != 0 else np.nan