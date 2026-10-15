from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Iterable, Protocol

import numpy as np
import numpy.typing as npt


FloatMatrix = npt.NDArray[np.float32]
IntVector = npt.NDArray[np.int64]


class Persistable(Protocol):
    def save(self, path: Path) -> None: ...


class RouterIndex(ABC):
    """Maps queries and data vectors to graph-derived storage partitions."""

    @abstractmethod
    def search_nodes(self, vectors: FloatMatrix, k: int) -> tuple[FloatMatrix, IntVector]:
        raise NotImplementedError

    @abstractmethod
    def route(self, vector: npt.ArrayLike, partitions: int) -> list[int]:
        raise NotImplementedError


class GraphPartitioner(ABC):
    """Partitions a weighted adjacency graph while balancing node weights."""

    @abstractmethod
    def partition(
        self,
        adjacency: list[list[int]],
        num_parts: int,
        node_weights: npt.ArrayLike | None = None,
    ) -> npt.NDArray[np.int32]:
        raise NotImplementedError


class VectorSource(ABC):
    """Streams source vectors without requiring the corpus in memory."""

    @property
    @abstractmethod
    def dimension(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def batches(self, batch_size: int) -> Iterable[tuple[IntVector, FloatMatrix]]:
        raise NotImplementedError

