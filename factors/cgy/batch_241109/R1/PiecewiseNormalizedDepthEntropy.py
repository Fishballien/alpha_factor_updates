import numpy as np

def PiecewiseNormalizedDepthEntropy(lob: np.array, num_pieces: int = 4) -> float:
    volumes = lob[:, 1]
    piece_size = max(1, len(volumes) // num_pieces)
    entropy = 0
    for i in range(0, len(volumes), piece_size):
        piece = volumes[i:i + piece_size]
        total_piece_volume = np.sum(piece)
        if total_piece_volume == 0:
            continue
        normalized_piece = piece / total_piece_volume
        entropy -= np.sum(normalized_piece * np.log(normalized_piece + 1e-9))
    return entropy
