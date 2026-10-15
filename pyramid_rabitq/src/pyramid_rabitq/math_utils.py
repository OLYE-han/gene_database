from __future__ import annotations

import numpy as np
import numpy.typing as npt


def as_float32_matrix(values: npt.ArrayLike, dimension: int | None = None) -> np.ndarray:
    result = np.asarray(values, dtype=np.float32)
    if result.ndim == 1:
        result = result.reshape(1, -1)
    if result.ndim != 2:
        raise ValueError("vectors must be a one- or two-dimensional array")
    if dimension is not None and result.shape[1] != dimension:
        raise ValueError(f"vector dimension {result.shape[1]}, expected {dimension}")
    if not np.isfinite(result).all():
        raise ValueError("vectors contain NaN or infinity")
    return np.ascontiguousarray(result)


def normalize_rows(values: npt.ArrayLike) -> np.ndarray:
    result = as_float32_matrix(values).copy()
    norms = np.linalg.norm(result, axis=1)
    if np.any(norms == 0):
        raise ValueError("zero vectors are not supported")
    result /= norms[:, None]
    return result


def prepare_vectors(values: npt.ArrayLike, metric: str) -> np.ndarray:
    result = as_float32_matrix(values)
    return normalize_rows(result) if metric == "cosine" else result

