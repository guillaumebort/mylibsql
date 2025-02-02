# mylibsql

mylibsql is a personal take on [libsql](https://github.com/tursodatabase/libsql), built for full control over SQLite replication—without a dedicated server. It uses **libsql’s virtual WAL** to support a **primary-replica** model where applications handle their own checkpointing and WAL log storage.

## Why mylibsql?

- **No server required** – Replication is fully managed by the embedding application.
- **Async durability** – Writes only resolve when safely checkpointed.
- **Checkpoint-based replication** – WAL logs are durably stored and applied to replicas as needed.
- **Built on libsql** – Compatible with SQLite, leveraging libsql virtual WAL for replication.

## How it works

1.	A **Primary** database is opened for reads and writes.
2.	Periodic **Checkpoints** generate WAL logs.
3.	These logs are **stored durably** (e.g., in an object store like S3).
4.	**Replicas** fetch and apply logs locally to stay in sync.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE.md) file for details.