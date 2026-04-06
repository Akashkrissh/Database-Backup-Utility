# Database Backup Utility

A command-line tool for automating database backup and restore operations. This utility supports MongoDB and PostgreSQL and is designed to simplify backup workflows for development and production environments.

---

## Overview

The Database Backup Utility provides a streamlined interface to perform backup and restore operations using native database tools. It supports compressed backups, structured logging, and an extensible architecture suitable for DevOps workflows and automation pipelines.

---

## Features

- Support for MongoDB and PostgreSQL  
- Command-line interface for backup and restore operations  
- Compressed backup files for efficient storage  
- Structured logging for monitoring and debugging  
- Simple and modular design for extensibility  

---

## Technology Stack

- **Language:** Python  
- **Libraries:** pymongo, psycopg2, tarfile, shutil, subprocess  
- **Database Tools:**
  - MongoDB: mongodump, mongorestore  
  - PostgreSQL: pg_dump, psql  

---

## Installation

```bash
git clone https://github.com/Akashkrissh/Database-Backup-Utility.git
cd Database-Backup-Utility

python -m venv venv

# Windows
venv\Scripts\activate

# Linux / macOS
source venv/bin/activate

pip install -r requirements.txt
```

---

## Prerequisites

Ensure the following are installed and accessible in your system PATH:

- Python 3.7 or higher  
- MongoDB Database Tools (mongodump, mongorestore)  
- PostgreSQL Client Tools (pg_dump, psql)  

---

## Usage

### Backup

**MongoDB**
```bash
python cli.py backup --db-type mongo --path ./backups/mongo
```

**PostgreSQL**
```bash
python cli.py backup --db-type postgres --path ./backups/postgres
```

---

### Restore

**MongoDB**
```bash
python cli.py restore --db-type mongo --backup-path ./backups/mongo/file.tar.gz
```

**PostgreSQL**
```bash
python cli.py restore --db-type postgres --backup-path ./backups/postgres/file.tar.gz
```

---

## Project Structure

```
Database-Backup-Utility/
│
├── src/
├── cli.py
├── requirements.txt
└── README.md
```

---

## Logging

The application provides structured logs for all operations, including:

- Backup start and completion  
- Restore operations  
- Error handling and failure cases  

Example:
```text
INFO  Backup process started
INFO  Backup completed successfully
ERROR Restore failed due to invalid file
```

---

## Use Cases

- Automating database backups in development and production environments  
- Integrating backup workflows into CI/CD pipelines  
- Supporting disaster recovery strategies  
- Managing database snapshots for testing and migration  

---

## Future Enhancements

- Support for MySQL and SQLite  
- Cloud storage integration (AWS S3, Google Cloud Storage, Azure Blob)  
- Backup scheduling using cron or task schedulers  
- Notification system integration (email or webhooks)  

---

## Contributing

Contributions are welcome. To contribute:

1. Fork the repository  
2. Create a feature branch  
3. Commit your changes  
4. Submit a pull request  

---