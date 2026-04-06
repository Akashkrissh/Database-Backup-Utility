"""AWS S3 / MinIO storage backend."""
from datetime import datetime
from typing import List
from .base import BaseStorage
from ..backup.compression import human_size
from ..logger import get_logger

logger = get_logger()


class S3Storage(BaseStorage):
    backend_name = "s3"

    def __init__(self, bucket: str, prefix: str = "dbvault/", region: str = "us-east-1",
                 access_key_id: str = "", secret_access_key: str = "", endpoint_url: str = None):
        self.bucket = bucket
        self.prefix = prefix
        self._s3 = self._build_client(region, access_key_id, secret_access_key, endpoint_url)

    def _build_client(self, region, access_key, secret_key, endpoint_url):
        try:
            import boto3
        except ImportError:
            raise ImportError("boto3 is required for S3 storage. Install: pip install boto3")
        kwargs = dict(region_name=region)
        if access_key and secret_key:
            kwargs["aws_access_key_id"] = access_key
            kwargs["aws_secret_access_key"] = secret_key
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        return boto3.client("s3", **kwargs)

    def _key(self, filename: str) -> str:
        return f"{self.prefix.rstrip('/')}/{filename}"

    def upload(self, local_path: str, filename: str) -> str:
        key = self._key(filename)
        logger.info("Uploading to S3: s3://%s/%s", self.bucket, key)
        self._s3.upload_file(local_path, self.bucket, key)
        return f"s3://{self.bucket}/{key}"

    def download(self, filename: str, dest_path: str) -> str:
        key = self._key(filename)
        self._s3.download_file(self.bucket, key, dest_path)
        return dest_path

    def delete(self, filename: str):
        key = self._key(filename)
        self._s3.delete_object(Bucket=self.bucket, Key=key)

    def list_backups(self) -> List[dict]:
        response = self._s3.list_objects_v2(Bucket=self.bucket, Prefix=self.prefix)
        results = []
        for obj in response.get("Contents", []):
            name = obj["Key"].split("/")[-1]
            if not name:
                continue
            results.append({
                "name": name,
                "size": human_size(obj["Size"]),
                "created": obj["LastModified"].strftime("%Y-%m-%d %H:%M:%S"),
                "type": "full" if "_full_" in name else "incremental" if "_incremental_" in name else "—",
            })
        return sorted(results, key=lambda x: x["created"], reverse=True)
