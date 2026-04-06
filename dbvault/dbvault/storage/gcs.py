"""Google Cloud Storage backend."""
from typing import List
from .base import BaseStorage
from ..backup.compression import human_size
from ..logger import get_logger

logger = get_logger()


class GCSStorage(BaseStorage):
    backend_name = "gcs"

    def __init__(self, bucket: str, prefix: str = "dbvault/", credentials_file: str = ""):
        self.bucket_name = bucket
        self.prefix = prefix
        self._client = self._build_client(credentials_file)
        self._bucket = self._client.bucket(bucket)

    def _build_client(self, credentials_file):
        try:
            from google.cloud import storage
        except ImportError:
            raise ImportError("google-cloud-storage required. Install: pip install google-cloud-storage")
        if credentials_file:
            return storage.Client.from_service_account_json(credentials_file)
        return storage.Client()  # uses GOOGLE_APPLICATION_CREDENTIALS env var

    def _blob_name(self, filename: str) -> str:
        return f"{self.prefix.rstrip('/')}/{filename}"

    def upload(self, local_path: str, filename: str) -> str:
        blob = self._bucket.blob(self._blob_name(filename))
        logger.info("Uploading to GCS: gs://%s/%s", self.bucket_name, blob.name)
        blob.upload_from_filename(local_path)
        return f"gs://{self.bucket_name}/{blob.name}"

    def download(self, filename: str, dest_path: str) -> str:
        blob = self._bucket.blob(self._blob_name(filename))
        blob.download_to_filename(dest_path)
        return dest_path

    def delete(self, filename: str):
        blob = self._bucket.blob(self._blob_name(filename))
        blob.delete()

    def list_backups(self) -> List[dict]:
        blobs = list(self._client.list_blobs(self.bucket_name, prefix=self.prefix))
        results = []
        for b in blobs:
            name = b.name.split("/")[-1]
            if not name:
                continue
            results.append({
                "name": name,
                "size": human_size(b.size),
                "created": b.time_created.strftime("%Y-%m-%d %H:%M:%S"),
                "type": "full" if "_full_" in name else "—",
            })
        return sorted(results, key=lambda x: x["created"], reverse=True)
