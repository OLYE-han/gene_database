"""Exact-distance, deterministic overlapping partition research baseline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import math

import numpy as np

from .data import validate_ids


def prepare(vectors, metric: str) -> np.ndarray:
    x = np.array(vectors, dtype=np.float64, copy=True)
    if x.ndim != 2 or x.shape[1] == 0 or not np.isfinite(x).all():
        raise ValueError("Vectors must be a finite 2-D matrix")
    if metric not in ("l2", "cosine"):
        raise ValueError("metric must be l2 or cosine")
    if metric == "cosine":
        norms = np.linalg.norm(x, axis=1)
        if np.any(norms == 0) or not np.isfinite(norms).all():
            raise ValueError("Cosine requires finite, nonzero norms")
        x /= norms[:, None]
    return x


def squared_distances(x: np.ndarray, centers: np.ndarray) -> np.ndarray:
    """Stable direct differences; does not materialize rows x centers x dim."""
    if x.ndim != 2 or centers.ndim != 2 or x.shape[1] != centers.shape[1]:
        raise ValueError("Incompatible vector and center dimensions")
    out = np.empty((len(x), len(centers)), dtype=np.float64)
    for j, center in enumerate(centers):
        delta = x - center
        out[:, j] = np.einsum("ij,ij->i", delta, delta)
    if not np.isfinite(out).all():
        raise ValueError("Non-finite squared distances")
    return out


@dataclass
class Model:
    centers: np.ndarray
    radii: np.ndarray
    metric: str
    iterations: int = 0
    converged: bool = False

    def __post_init__(self):
        self.centers = np.asarray(self.centers, dtype=np.float64)
        self.radii = np.asarray(self.radii, dtype=np.float64)
        if (self.metric not in ("l2", "cosine") or self.centers.ndim != 2
                or min(self.centers.shape) == 0 or self.radii.shape != (len(self.centers),)
                or not np.isfinite(self.centers).all() or not np.isfinite(self.radii).all()
                or np.any(self.radii < 0)):
            raise ValueError("Invalid partition model")
        distances = np.sqrt(squared_distances(self.centers, self.centers))
        scale = max(float(np.linalg.norm(self.centers, axis=1).max()),
                    float(self.radii.max()), np.finfo(float).tiny)
        offdiag = ~np.eye(len(self.centers), dtype=bool)
        if np.any(distances[offdiag] <= 1e-12 * scale):
            raise ValueError("Coincident or near-coincident centers; retrain the model")

    def scores(self, vectors, method="margin"):
        x = prepare(vectors, self.metric)
        distances = squared_distances(x, self.centers)
        primary = distances.argmin(axis=1)
        gaps = distances - distances[np.arange(len(x)), primary, None]
        pair_dist = np.sqrt(squared_distances(self.centers, self.centers))
        positive = self.radii[self.radii > 0]
        scale = float(np.median(positive)) if len(positive) else float(pair_dist.max())
        floor = max(scale * 1e-12, np.finfo(float).tiny)
        if method in ("margin", "boundary"):
            denom = 2 * pair_dist[primary]
            if method == "margin":
                denom *= np.maximum((self.radii[primary, None] + self.radii[None, :]) / 2, floor)
            denom[np.arange(len(x)), primary] = 1
            scores = gaps / denom
        elif method == "relative_gap":
            scores = gaps / np.maximum(distances[np.arange(len(x)), primary, None], floor**2)
        else:
            raise ValueError("Unknown overlap scoring method")
        scores[np.arange(len(x)), primary] = np.inf
        return primary, scores

    def save(self, path):
        with open(path, "xb") as handle:
            np.savez(handle, version=np.array(1), centers=self.centers, radii=self.radii,
                     metric=np.array(self.metric), iterations=np.array(self.iterations),
                     converged=np.array(self.converged))

    @classmethod
    def load(cls, path):
        with np.load(path, allow_pickle=False) as saved:
            if int(saved["version"]) != 1:
                raise ValueError("Unsupported model version")
            return cls(saved["centers"], saved["radii"], str(saved["metric"]),
                       int(saved["iterations"]), bool(saved["converged"]))


def fit(vectors, shards: int, metric: str, seed=42, max_iterations=30, tolerance=1e-6):
    x = prepare(vectors, metric)
    if shards < 1 or shards > len(x) or max_iterations < 1 or tolerance < 0:
        raise ValueError("Invalid k-means parameters")
    rng = np.random.default_rng(seed)
    centers = [x[int(rng.integers(len(x)))].copy()]
    nearest = squared_distances(x, np.array(centers))[:, 0]
    for _ in range(1, shards):
        total = float(nearest.sum())
        if total <= 0 or not math.isfinite(total):
            raise ValueError("Insufficient distinct vectors for requested shards")
        chosen = int(rng.choice(len(x), p=nearest / total))
        centers.append(x[chosen].copy())
        nearest = np.minimum(nearest, squared_distances(x, np.array([centers[-1]]))[:, 0])
    centers = np.array(centers)
    converged = False
    for iteration in range(1, max_iterations + 1):
        distances = squared_distances(x, centers)
        labels = distances.argmin(axis=1)
        updated = centers.copy()
        farthest = np.argsort(-distances.min(axis=1), kind="stable")
        used = set()
        for j in range(shards):
            members = x[labels == j]
            if len(members):
                updated[j] = members.mean(axis=0)
            else:
                candidate = next(int(i) for i in farthest if int(i) not in used)
                used.add(candidate)
                updated[j] = x[candidate]
        shift = np.linalg.norm(updated - centers)
        reference = max(np.linalg.norm(centers), np.finfo(float).tiny)
        centers = updated
        if shift <= tolerance * reference:
            converged = True
            break
    distances = squared_distances(x, centers)
    labels = distances.argmin(axis=1)
    if np.any(np.bincount(labels, minlength=shards) == 0):
        raise ValueError("Empty final cluster; change seed or sample")
    radii = np.array([np.sqrt(distances[labels == j, j].mean()) for j in range(shards)])
    return Model(centers, radii, metric, iteration, converged)


def hash_ids(ids, seed):
    """SplitMix64 tie break; preserves full uint64 identity with ID fallback."""
    ids = validate_ids(ids)
    if not 0 <= seed < 2**64:
        raise ValueError("seed outside uint64")
    with np.errstate(over="ignore"):
        x = ids ^ np.uint64(seed)
        x = x + np.uint64(0x9E3779B97F4A7C15)
        x = (x ^ (x >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
        x = (x ^ (x >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
        return x ^ (x >> np.uint64(31))


@dataclass(frozen=True)
class Threshold:
    mode: str
    score: float = 0.0
    tie_hash: int = 0
    tie_id: int = 0
    seed: int = 42

    def __post_init__(self):
        if (self.mode not in ("none", "all", "cutoff") or not math.isfinite(self.score)
                or self.score < 0 or any(not isinstance(v, int) or not 0 <= v < 2**64
                                        for v in (self.tie_hash, self.tie_id, self.seed))):
            raise ValueError("Invalid overlap threshold")

    def to_dict(self):
        return asdict(self)


def calibrate(scores, ids, rho: float, seed=42) -> Threshold:
    ids = validate_ids(ids)
    scores = np.asarray(scores, dtype=float)
    if (not math.isfinite(rho) or not 0 <= rho <= 1 or scores.ndim != 2
            or len(scores) != len(ids) or len(ids) == 0 or scores.shape[1] == 0
            or np.isnan(scores).any() or np.any(scores < 0)
            or len(np.unique(ids)) != len(ids)):
        raise ValueError("Invalid calibration input")
    hashes = hash_ids(ids, seed)
    if rho == 0 or scores.shape[1] == 1:
        return Threshold("none", seed=seed)
    if rho == 1:
        return Threshold("all", seed=seed)
    count = math.floor(rho * len(ids))
    if count == 0:
        return Threshold("none", seed=seed)
    boundary = scores.min(axis=1)
    if not np.isfinite(boundary).all():
        raise ValueError("Missing finite secondary-center score")
    idx = np.lexsort((ids, hashes, boundary))[count - 1]
    return Threshold("cutoff", float(boundary[idx]), int(hashes[idx]), int(ids[idx]), seed)


def memberships(primary, scores, ids, threshold: Threshold, max_copies=3):
    ids = validate_ids(ids)
    scores = np.asarray(scores, dtype=float)
    primary = np.asarray(primary)
    if (scores.ndim != 2 or len(scores) != len(ids) or primary.shape != (len(ids),)
            or primary.dtype.kind not in "iu" or np.any(primary < 0)
            or np.any(primary >= scores.shape[1]) or max_copies < 1
            or np.isnan(scores).any() or np.any(scores < 0)):
        raise ValueError("Invalid membership input")
    n, k = scores.shape
    selected = np.zeros((n, k), dtype=bool)
    selected[np.arange(n), primary] = True
    if threshold.mode == "none" or max_copies == 1 or k == 1:
        return selected
    if threshold.mode == "all":
        eligible = np.isfinite(scores)
    elif threshold.mode == "cutoff":
        hashes = hash_ids(ids, threshold.seed)
        tie = (hashes < threshold.tie_hash) | ((hashes == threshold.tie_hash) & (ids <= threshold.tie_id))
        eligible = (scores < threshold.score) | ((scores == threshold.score) & tie[:, None])
    else:
        raise ValueError("Unknown threshold mode")
    eligible[np.arange(n), primary] = False
    order = np.argsort(scores, axis=1, kind="stable")[:, :min(max_copies - 1, k - 1)]
    rows = np.arange(n)[:, None]
    selected[rows, order] |= eligible[rows, order]
    return selected


def summary(selected, primary):
    copies = selected.sum(axis=1)
    counts = selected.sum(axis=0)
    return {
        "rows": len(selected), "replication_factor": float(copies.mean()),
        "overlap_fraction_observed": float(np.mean(copies > 1)),
        "primary_counts": np.bincount(primary, minlength=selected.shape[1]).tolist(),
        "replica_counts": counts.tolist(),
        "max_to_mean_replica_load": float(counts.max() / counts.mean()),
    }
