import numpy as np

def PiecewisePriceGapEntropy(lob: np.array, num_pieces: int = 4) -> float:
    prices = lob[:, 0]
    price_gaps = np.abs(np.diff(prices))
    chunk_size = max(1, len(price_gaps) // num_pieces)
    entropy = 0
    for i in range(0, len(price_gaps), chunk_size):
        chunk = price_gaps[i:i + chunk_size]
        total_chunk = np.sum(chunk)
        if total_chunk == 0:
            continue
        chunk_probs = chunk / total_chunk
        entropy -= np.sum(chunk_probs * np.log(chunk_probs + 1e-9))
    return entropy
