"""Abstract base class for all storage backends."""
from __future__ import annotations
import abc


class BaseStorage(abc.ABC):
    @abc.abstractmethod
    def upload(self, local_path: str, remote_name: str) -> str:
        """Upload/move local_path to backend. Returns location string."""

    def download(self, remote_name: str, local_path: str) -> str:
        raise NotImplementedError(f"{type(self).__name__} does not implement download().")
