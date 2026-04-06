"""Connector factory — maps db_type string to connector class."""
from __future__ import annotations
from .base import BaseConnector


class ConnectorFactory:
    @staticmethod
    def create(db_type: str, params: dict) -> BaseConnector:
        from .mysql import MySQLConnector
        from .postgresql import PostgreSQLConnector
        from .mongodb import MongoDBConnector
        from .sqlite import SQLiteConnector
        registry = {
            "mysql": MySQLConnector,
            "postgresql": PostgreSQLConnector,
            "postgres": PostgreSQLConnector,
            "mongodb": MongoDBConnector,
            "mongo": MongoDBConnector,
            "sqlite": SQLiteConnector,
        }
        key = db_type.lower().strip()
        cls = registry.get(key)
        if cls is None:
            raise ValueError(
                f"Unknown database type '{db_type}'. "
                f"Supported: {', '.join(sorted(set(registry.keys())))}."
            )
        return cls(dict(params, db_type=key))

    @staticmethod
    def supported_types():
        return ["mysql", "postgresql", "mongodb", "sqlite"]
