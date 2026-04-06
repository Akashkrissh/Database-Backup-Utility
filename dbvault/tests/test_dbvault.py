"""
Unit tests for DBVault.

Run with:  pytest tests/ -v
"""

from __future__ import annotations

import gzip
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock, patch


# ── connector factory ──────────────────────────────────────────────────────────

class TestConnectorFactory(unittest.TestCase):

    def test_creates_mysql(self):
        from dbvault.connectors.factory import ConnectorFactory
        from dbvault.connectors.mysql import MySQLConnector
        c = ConnectorFactory.create("mysql", {"database": "test"})
        self.assertIsInstance(c, MySQLConnector)

    def test_creates_postgresql(self):
        from dbvault.connectors.factory import ConnectorFactory
        from dbvault.connectors.postgresql import PostgreSQLConnector
        c = ConnectorFactory.create("postgresql", {"database": "test"})
        self.assertIsInstance(c, PostgreSQLConnector)

    def test_postgres_alias(self):
        from dbvault.connectors.factory import ConnectorFactory
        from dbvault.connectors.postgresql import PostgreSQLConnector
        c = ConnectorFactory.create("postgres", {"database": "test"})
        self.assertIsInstance(c, PostgreSQLConnector)

    def test_creates_mongodb(self):
        from dbvault.connectors.factory import ConnectorFactory
        from dbvault.connectors.mongodb import MongoDBConnector
        c = ConnectorFactory.create("mongodb", {"database": "test"})
        self.assertIsInstance(c, MongoDBConnector)

    def test_creates_sqlite(self):
        from dbvault.connectors.factory import ConnectorFactory
        from dbvault.connectors.sqlite import SQLiteConnector
        c = ConnectorFactory.create("sqlite", {"database": ":memory:"})
        self.assertIsInstance(c, SQLiteConnector)

    def test_unknown_type_raises(self):
        from dbvault.connectors.factory import ConnectorFactory
        with self.assertRaises(ValueError):
            ConnectorFactory.create("oracle", {"database": "test"})


# ── SQLite connector ───────────────────────────────────────────────────────────

class TestSQLiteConnector(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "test.db")
        # Seed a small database
        con = sqlite3.connect(self.db_path)
        con.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        con.execute("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');")
        con.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER);")
        con.execute("INSERT INTO orders VALUES (100, 1), (101, 2);")
        con.commit()
        con.close()

    def _connector(self):
        from dbvault.connectors.sqlite import SQLiteConnector
        return SQLiteConnector({"database": self.db_path, "db_type": "sqlite"})

    def test_connection_success(self):
        c = self._connector()
        info = c.test_connection()
        self.assertIn("SQLite", info)

    def test_connection_missing_file(self):
        from dbvault.connectors.sqlite import SQLiteConnector
        c = SQLiteConnector({"database": "/nonexistent/path/db.sqlite", "db_type": "sqlite"})
        with self.assertRaises(RuntimeError):
            c.test_connection()

    def test_full_backup_and_restore(self):
        c = self._connector()
        backup_path = os.path.join(self.tmpdir, "backup.db")
        c.backup_full(backup_path)
        self.assertTrue(os.path.isfile(backup_path))

        # Verify backup contains same data
        con = sqlite3.connect(backup_path)
        rows = con.execute("SELECT * FROM users ORDER BY id;").fetchall()
        con.close()
        self.assertEqual(rows, [(1, "Alice"), (2, "Bob")])

    def test_partial_backup_tables(self):
        c = self._connector()
        backup_path = os.path.join(self.tmpdir, "partial.db")
        c.backup_full(backup_path, tables=["users"])

        con = sqlite3.connect(backup_path)
        tables = con.execute(
            "SELECT name FROM sqlite_master WHERE type='table';"
        ).fetchall()
        con.close()
        table_names = [t[0] for t in tables]
        self.assertIn("users", table_names)
        self.assertNotIn("orders", table_names)

    def test_selective_restore(self):
        c = self._connector()
        backup_path = os.path.join(self.tmpdir, "backup.db")
        c.backup_full(backup_path)

        # Create a fresh target DB
        new_db = os.path.join(self.tmpdir, "new.db")
        con = sqlite3.connect(new_db)
        con.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT);")
        con.commit()
        con.close()

        from dbvault.connectors.sqlite import SQLiteConnector
        c2 = SQLiteConnector({"database": new_db, "db_type": "sqlite"})
        c2.restore_full(backup_path, tables=["users"])

        con = sqlite3.connect(new_db)
        rows = con.execute("SELECT * FROM users ORDER BY id;").fetchall()
        con.close()
        self.assertEqual(rows, [(1, "Alice"), (2, "Bob")])

    def test_dry_run_valid_file(self):
        c = self._connector()
        backup_path = os.path.join(self.tmpdir, "backup.db")
        c.backup_full(backup_path)
        # Should not raise
        c.restore_full(backup_path, dry_run=True)

    def test_dry_run_invalid_file(self):
        c = self._connector()
        bad_path = os.path.join(self.tmpdir, "bad.db")
        with open(bad_path, "w") as f:
            f.write("this is not sqlite")
        with self.assertRaises(RuntimeError):
            c.restore_full(bad_path, dry_run=True)


# ── BackupManager ──────────────────────────────────────────────────────────────

class TestBackupManager(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        # Create a real SQLite DB for the connector
        self.db_path = os.path.join(self.tmpdir, "app.db")
        con = sqlite3.connect(self.db_path)
        con.execute("CREATE TABLE items (id INTEGER PRIMARY KEY, val TEXT);")
        con.execute("INSERT INTO items VALUES (1, 'x');")
        con.commit()
        con.close()

    def _make_manager(self, compress=True):
        from dbvault.connectors.sqlite import SQLiteConnector
        from dbvault.storage.local import LocalStorage
        from dbvault.backup.manager import BackupManager
        connector = SQLiteConnector({"database": self.db_path, "db_type": "sqlite"})
        storage = LocalStorage({"output_dir": self.tmpdir})
        return BackupManager(connector, storage)

    def test_full_backup_creates_file(self):
        mgr = self._make_manager()
        result = mgr.run_backup(backup_type="full", compress=False)
        self.assertTrue(os.path.isfile(result["location"]))
        self.assertIn("app", result["filename"])
        self.assertGreater(result["size_bytes"], 0)

    def test_full_backup_compressed(self):
        mgr = self._make_manager()
        result = mgr.run_backup(backup_type="full", compress=True)
        self.assertTrue(result["filename"].endswith(".gz"))
        # Verify it's a valid gzip
        with gzip.open(result["location"], "rb") as f:
            data = f.read()
        self.assertGreater(len(data), 0)

    def test_backup_result_keys(self):
        mgr = self._make_manager()
        result = mgr.run_backup(backup_type="full", compress=False)
        for key in ("filename", "location", "size_bytes", "size_human",
                    "duration_s", "backup_type", "timestamp_utc"):
            self.assertIn(key, result)

    def test_notifier_called_on_success(self):
        from dbvault.connectors.sqlite import SQLiteConnector
        from dbvault.storage.local import LocalStorage
        from dbvault.backup.manager import BackupManager
        notifier = MagicMock()
        connector = SQLiteConnector({"database": self.db_path, "db_type": "sqlite"})
        storage = LocalStorage({"output_dir": self.tmpdir})
        mgr = BackupManager(connector, storage, notifier=notifier)
        mgr.run_backup(backup_type="full", compress=False)
        notifier.send_success.assert_called_once()

    def test_notifier_called_on_failure(self):
        from dbvault.connectors.sqlite import SQLiteConnector
        from dbvault.storage.local import LocalStorage
        from dbvault.backup.manager import BackupManager
        notifier = MagicMock()
        connector = SQLiteConnector({"database": "/bad/path.db", "db_type": "sqlite"})
        storage = LocalStorage({"output_dir": self.tmpdir})
        mgr = BackupManager(connector, storage, notifier=notifier)
        with self.assertRaises(Exception):
            mgr.run_backup()
        notifier.send_failure.assert_called_once()


# ── RestoreManager ─────────────────────────────────────────────────────────────

class TestRestoreManager(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        self.db_path = os.path.join(self.tmpdir, "app.db")
        con = sqlite3.connect(self.db_path)
        con.execute("CREATE TABLE data (id INTEGER PRIMARY KEY, val TEXT);")
        con.execute("INSERT INTO data VALUES (1, 'hello');")
        con.commit()
        con.close()

    def test_restore_from_plain_backup(self):
        from dbvault.connectors.sqlite import SQLiteConnector
        from dbvault.backup.manager import BackupManager
        from dbvault.restore.manager import RestoreManager
        from dbvault.storage.local import LocalStorage

        # Backup
        connector = SQLiteConnector({"database": self.db_path, "db_type": "sqlite"})
        storage = LocalStorage({"output_dir": self.tmpdir})
        mgr = BackupManager(connector, storage)
        result = mgr.run_backup(compress=False)

        # Restore to a new file
        new_db = os.path.join(self.tmpdir, "restored.db")
        connector2 = SQLiteConnector({"database": new_db, "db_type": "sqlite"})
        rm = RestoreManager(connector2)
        restore_result = rm.run_restore(result["location"])
        self.assertEqual(restore_result["status"], "ok")

        con = sqlite3.connect(new_db)
        rows = con.execute("SELECT * FROM data;").fetchall()
        con.close()
        self.assertEqual(rows, [(1, "hello")])

    def test_restore_from_gz_backup(self):
        from dbvault.connectors.sqlite import SQLiteConnector
        from dbvault.backup.manager import BackupManager
        from dbvault.restore.manager import RestoreManager
        from dbvault.storage.local import LocalStorage

        connector = SQLiteConnector({"database": self.db_path, "db_type": "sqlite"})
        storage = LocalStorage({"output_dir": self.tmpdir})
        mgr = BackupManager(connector, storage)
        result = mgr.run_backup(compress=True)

        new_db = os.path.join(self.tmpdir, "restored_gz.db")
        connector2 = SQLiteConnector({"database": new_db, "db_type": "sqlite"})
        rm = RestoreManager(connector2)
        restore_result = rm.run_restore(result["location"])
        self.assertEqual(restore_result["status"], "ok")


# ── storage backends ───────────────────────────────────────────────────────────

class TestLocalStorage(unittest.TestCase):

    def test_upload_creates_file(self):
        from dbvault.storage.local import LocalStorage
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "test.bak")
            with open(src, "w") as f:
                f.write("backup data")
            out_dir = os.path.join(tmp, "backups")
            storage = LocalStorage({"output_dir": out_dir})
            location = storage.upload(src, "test.bak")
            self.assertTrue(os.path.isfile(location))
            self.assertEqual(open(location).read(), "backup data")

    def test_download_retrieves_file(self):
        from dbvault.storage.local import LocalStorage
        with tempfile.TemporaryDirectory() as tmp:
            out_dir = os.path.join(tmp, "backups")
            storage = LocalStorage({"output_dir": out_dir})
            src = os.path.join(out_dir, "myfile.bak")
            with open(src, "w") as f:
                f.write("contents")
            dest = os.path.join(tmp, "download.bak")
            storage.download("myfile.bak", dest)
            self.assertEqual(open(dest).read(), "contents")


# ── config manager ─────────────────────────────────────────────────────────────

class TestConfigManager(unittest.TestCase):

    def test_merge_cli_wins(self):
        from dbvault.utils.config import ConfigManager
        cm = ConfigManager()
        merged = cm.merge(host="cli-host", database="mydb")
        self.assertEqual(merged["host"], "cli-host")

    def test_merge_default_port_mysql(self):
        from dbvault.utils.config import ConfigManager
        cm = ConfigManager()
        merged = cm.merge(db_type="mysql", database="test", port=None)
        self.assertEqual(merged["port"], 3306)

    def test_merge_default_port_pg(self):
        from dbvault.utils.config import ConfigManager
        cm = ConfigManager()
        merged = cm.merge(db_type="postgresql", database="test", port=None)
        self.assertEqual(merged["port"], 5432)

    def test_load_json_config(self):
        from dbvault.utils.config import ConfigManager
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as f:
            import json
            json.dump({"database": {"host": "jsonhost", "port": 9999}}, f)
            fname = f.name
        try:
            cm = ConfigManager(fname)
            merged = cm.merge(database="test")
            self.assertEqual(merged.get("host"), "jsonhost")
        finally:
            os.unlink(fname)

    def test_missing_config_file_raises(self):
        from dbvault.utils.config import ConfigManager
        with self.assertRaises(FileNotFoundError):
            ConfigManager("/nonexistent/config.yaml")


# ── listing utility ────────────────────────────────────────────────────────────

class TestListing(unittest.TestCase):

    def test_list_backups(self):
        from dbvault.utils.listing import list_local_backups
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("mydb_full_20240101.sql.gz", "mydb_full_20240102.sql.gz", "readme.txt"):
                open(os.path.join(tmp, name), "w").write("x")
            results = list_local_backups(tmp)
            names = [r["filename"] for r in results]
            self.assertIn("mydb_full_20240101.sql.gz", names)
            self.assertIn("mydb_full_20240102.sql.gz", names)
            self.assertNotIn("readme.txt", names)

    def test_db_filter(self):
        from dbvault.utils.listing import list_local_backups
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("app_full.sql.gz", "shop_full.sql.gz"):
                open(os.path.join(tmp, name), "w").write("x")
            results = list_local_backups(tmp, db_filter="app")
            names = [r["filename"] for r in results]
            self.assertIn("app_full.sql.gz", names)
            self.assertNotIn("shop_full.sql.gz", names)

    def test_empty_dir(self):
        from dbvault.utils.listing import list_local_backups
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(list_local_backups(tmp), [])

    def test_missing_dir(self):
        from dbvault.utils.listing import list_local_backups
        self.assertEqual(list_local_backups("/nonexistent/dir"), [])


if __name__ == "__main__":
    unittest.main()
