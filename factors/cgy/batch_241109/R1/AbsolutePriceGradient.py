import numpy as np

def AbsolutePriceGradient(lob: np.array) -> float:
    abs_gradient_sum = 0.0
    best_price = lob[0, 0]
    for i in range(1, lob.shape[0]):
        current_price = lob[i, 0]
        current_volume = lob[i, 1]
        price_gradient = abs(current_price - lob[i - 1, 0]) / best_price
        abs_gradient_sum += price_gradient * current_volume

    return abs_gradient_sum
