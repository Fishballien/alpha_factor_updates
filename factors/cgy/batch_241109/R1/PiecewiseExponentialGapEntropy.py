import numpy as np

def PiecewiseExponentialGapEntropy(lob: np.array, num_pieces: int = 3, decay_rate: float = 0.1) -> float:
    prices = lob[:, 0]
    price_gaps = np.abs(np.diff(prices))
    piece_size = max(1, len(price_gaps) // num_pieces)
    entropy = 0
    for i in range(0, len(price_gaps), piece_size):
        piece = price_gaps[i:i + piece_size]
        decay_weights = np.exp(-decay_rate * np.arange(len(piece)))
        weighted_piece = piece * decay_weights
        total_weighted_piece = np.sum(weighted_piece)
        if total_weighted_piece == 0:
            continue
        piece_probs = weighted_piece / total_weighted_piece
        entropy -= np.sum(piece_probs * np.log(piece_probs + 1e-9))
    return entropy
