from __future__ import annotations

from dataclasses import asdict, dataclass

import numpy as np

from .engine import ReadNodeEngine
from .math_utils import prepare_vectors
from .storage import H5Corpus


@dataclass(frozen=True)
class RecallReport:
    queries: int
    top_k: int
    mean_recall: float
    minimum_recall: float
    per_query_recall: list[float]

    def as_dict(self):
        return asdict(self)


def _exact_top_k(corpus: H5Corpus, query: np.ndarray, metric: str, top_k: int, batch_size: int):
    best_distances = np.empty(0, dtype=np.float32)
    best_ids = np.empty(0, dtype=np.int64)
    for ids, vectors in corpus.batches(batch_size):
        prepared = prepare_vectors(vectors, metric)
        if metric == "cosine":
            distances = 1.0 - prepared @ query
        else:
            distances = np.sum((prepared - query) ** 2, axis=1)
        merged_distances = np.concatenate((best_distances, distances))
        merged_ids = np.concatenate((best_ids, ids))
        keep = min(top_k, len(merged_ids))
        selected = np.argpartition(merged_distances, keep - 1)[:keep]
        best_distances = merged_distances[selected]
        best_ids = merged_ids[selected]
    order = np.argsort(best_distances)
    return best_ids[order], best_distances[order]


def evaluate_recall(
    engine: ReadNodeEngine,
    corpus: H5Corpus,
    queries: int = 100,
    top_k: int = 10,
    batch_size: int = 4096,
) -> RecallReport:
    query_vectors = corpus.sample(queries, seed=engine.config.seed + 10_000)
    recalls: list[float] = []
    for vector in query_vectors:
        query = prepare_vectors(vector, engine.config.metric)[0]
        expected, _ = _exact_top_k(corpus, query, engine.config.metric, top_k, batch_size)
        actual = engine.search(vector, top_k).hits
        recalls.append(len(set(expected.tolist()) & {hit.id for hit in actual}) / top_k)
    return RecallReport(
        queries=len(recalls),
        top_k=top_k,
        mean_recall=float(np.mean(recalls)),
        minimum_recall=float(np.min(recalls)),
        per_query_recall=recalls,
    )

