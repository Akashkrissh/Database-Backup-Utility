"""Storage backend factory."""

from __future__ import annotations

from .base import BaseStorage


class StorageFactory:
    """Factory for creating storage backend instances."""

    @staticmethod
    def create(backend: str, params: dict) -> BaseStorage:
        """
        Instantiate the storage backend for *backend*.

        Parameters
        ----------
        backend : str
            One of: local, s3, gcs, azure.
        params : dict
            Backend-specific parameters.

        Returns
        -------
        BaseStorage

        Raises
        ------
        ValueError
            If *backend* is not recognised.
        """
        key = backend.lower().strip()

        if key == "local":
            from .local import LocalStorage
            return LocalStorage(params)

        if key == "s3":
            from .s3 import S3Storage
            return S3Storage(params)

        if key in ("gcs", "google", "google_cloud"):
            from .gcs import GCSStorage
            return GCSStorage(params)

        if key in ("azure", "azure_blob"):
            from .azure import AzureStorage
            return AzureStorage(params)

        raise ValueError(
            f"Unknown storage backend '{backend}'. "
            "Supported: local, s3, gcs, azure."
        )
