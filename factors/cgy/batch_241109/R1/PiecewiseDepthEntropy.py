import numpy as np

def PiecewiseDepthEntropy(lob: np.array, num_pieces: int = 5) -> float:
    volumes = lob[:, 1]
    chunk_size = max(1, len(volumes) // num_pieces)
    entropy = 0
    for i in range(0, len(volumes), chunk_size):
        chunk = volumes[i:i + chunk_size]
        total_chunk = np.sum(chunk)
        if total_chunk == 0:
            continue
        chunk_probs = chunk / total_chunk
        entropy -= np.sum(chunk_probs * np.log(chunk_probs + 1e-9))
    return entropy
