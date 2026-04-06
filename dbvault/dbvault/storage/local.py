"""Local filesystem storage backend."""
from __future__ import annotations
import os, shutil
from .base import BaseStorage


class LocalStorage(BaseStorage):
    def __init__(self, params: dict):
        self.output_dir = params.get("output_dir", "./backups")
        os.makedirs(self.output_dir, exist_ok=True)

    def upload(self, local_path: str, remote_name: str) -> str:
        dest = os.path.join(self.output_dir, remote_name)
        if os.path.abspath(local_path) != os.path.abspath(dest):
            shutil.copy2(local_path, dest)
        return os.path.abspath(dest)

    def download(self, remote_name: str, local_path: str) -> str:
        src = os.path.join(self.output_dir, remote_name)
        if not os.path.exists(src):
            raise FileNotFoundError(f"Backup not found: {src}")
        shutil.copy2(src, local_path)
        return local_path
