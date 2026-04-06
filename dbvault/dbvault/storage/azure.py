"""Azure Blob Storage backend."""
from typing import List
from .base import BaseStorage
from ..backup.compression import human_size
from ..logger import get_logger

logger = get_logger()


class AzureStorage(BaseStorage):
    backend_name = "azure"

    def __init__(self, account_name: str, account_key: str, container: str, prefix: str = "dbvault/"):
        self.container = container
        self.prefix = prefix
        self._client = self._build_client(account_name, account_key)

    def _build_client(self, account_name, account_key):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError:
            raise ImportError("azure-storage-blob required. Install: pip install azure-storage-blob")
        conn = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
        return BlobServiceClient.from_connection_string(conn)

    def _blob_name(self, filename: str) -> str:
        return f"{self.prefix.rstrip('/')}/{filename}"

    def upload(self, local_path: str, filename: str) -> str:
        blob_name = self._blob_name(filename)
        logger.info("Uploading to Azure Blob: %s/%s", self.container, blob_name)
        blob_client = self._client.get_blob_client(container=self.container, blob=blob_name)
        with open(local_path, "rb") as f:
            blob_client.upload_blob(f, overwrite=True)
        return f"https://{self._client.account_name}.blob.core.windows.net/{self.container}/{blob_name}"

    def download(self, filename: str, dest_path: str) -> str:
        blob_client = self._client.get_blob_client(container=self.container, blob=self._blob_name(filename))
        with open(dest_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
        return dest_path

    def delete(self, filename: str):
        blob_client = self._client.get_blob_client(container=self.container, blob=self._blob_name(filename))
        blob_client.delete_blob()

    def list_backups(self) -> List[dict]:
        container_client = self._client.get_container_client(self.container)
        results = []
        for b in container_client.list_blobs(name_starts_with=self.prefix):
            name = b.name.split("/")[-1]
            if not name:
                continue
            results.append({
                "name": name,
                "size": human_size(b.size),
                "created": b.last_modified.strftime("%Y-%m-%d %H:%M:%S"),
                "type": "full" if "_full_" in name else "—",
            })
        return sorted(results, key=lambda x: x["created"], reverse=True)
