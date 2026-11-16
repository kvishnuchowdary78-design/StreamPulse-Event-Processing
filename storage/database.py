
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path


DEFAULT_DATABASE = Path("data/output/streampulse.db")


def get_connection(database_file=None):
    database_file = Path(database_file or DEFAULT_DATABASE)
    database_file.parent.mkdir(parents=True, exist_ok=True)

    connection = sqlite3.connect(database_file)
    connection.execute("PRAGMA foreign_keys = ON")

    return connection


def ensure_column(connection, table_name, column_name, column_type):
    """Add a column only when it does not already exist."""

    columns = {
        row[1]
        for row in connection.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()
    }

    if column_name not in columns:
        connection.execute(
            f"ALTER TABLE {table_name} "
            f"ADD COLUMN {column_name} {column_type}"
        )


def initialize_database(database_file=None):
    """Create tables and safely migrate existing databases."""

    connection = get_connection(database_file)

    try:
        with connection:
            connection.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    order_id TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    amount REAL NOT NULL,
                    event_time TEXT NOT NULL,
                    arrival_time TEXT NOT NULL,
                    is_late INTEGER NOT NULL,
                    source_offset INTEGER UNIQUE NOT NULL
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS rejected_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_offset INTEGER UNIQUE NOT NULL,
                    raw_data TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS duplicate_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_offset INTEGER UNIQUE NOT NULL,
                    event_id TEXT NOT NULL,
                    reason TEXT NOT NULL
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS checkpoints (
                    source_file TEXT PRIMARY KEY,
                    last_processed_offset INTEGER NOT NULL
                )
            """)

            connection.execute("""
                CREATE TABLE IF NOT EXISTS pipeline_runs (
                    run_id TEXT PRIMARY KEY,
                    source_file TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT,
                    status TEXT NOT NULL,
                    failure_reason TEXT
                )
            """)

            # Backward-compatible migration.
            # Existing records remain intact.
            for table_name in (
                "processed_events",
                "rejected_events",
                "duplicate_events"
            ):
                ensure_column(
                    connection,
                    table_name,
                    "source_file",
                    "TEXT"
                )

                ensure_column(
                    connection,
                    table_name,
                    "run_id",
                    "TEXT"
                )

        print("StreamPulse database initialized successfully.")

    finally:
        connection.close()


def create_pipeline_run(connection, source_file):
    """Create a new pipeline execution record."""

    run_id = str(uuid.uuid4())

    start_time = datetime.now(
        timezone.utc
    ).isoformat()

    with connection:
        connection.execute("""
            INSERT INTO pipeline_runs (
                run_id,
                source_file,
                start_time,
                status
            )
            VALUES (?, ?, ?, ?)
        """, (
            run_id,
            str(source_file),
            start_time,
            "RUNNING"
        ))

    return run_id


def finish_pipeline_run(
    connection,
    run_id,
    status,
    failure_reason=None
):
    """Mark a pipeline execution as successful or failed."""

    if status not in ("SUCCESS", "FAILED"):
        raise ValueError("Invalid pipeline run status")

    end_time = datetime.now(
        timezone.utc
    ).isoformat()

    with connection:
        cursor = connection.execute("""
            UPDATE pipeline_runs
            SET end_time = ?,
                status = ?,
                failure_reason = ?
            WHERE run_id = ?
        """, (
            end_time,
            status,
            failure_reason,
            run_id
        ))

        if cursor.rowcount != 1:
            raise ValueError(
                f"Pipeline run not found: {run_id}"
            )


def get_last_processed_offset(connection, source_file):
    result = connection.execute("""
        SELECT last_processed_offset
        FROM checkpoints
        WHERE source_file = ?
    """, (source_file,)).fetchone()

    return result[0] if result else 0


def get_processing_state(connection):
    rows = connection.execute(
        "SELECT event_id FROM processed_events"
    ).fetchall()

    seen_event_ids = {row[0] for row in rows}

    result = connection.execute(
        "SELECT MAX(event_time) FROM processed_events"
    ).fetchone()

    max_event_time = (
        datetime.fromisoformat(result[0])
        if result[0] is not None
        else None
    )

    return seen_event_ids, max_event_time


def insert_event_row(
    connection,
    table_name,
    values,
    source_file=None,
    run_id=None
):
    """
    Insert an event outcome.

    Metadata columns are included when available.
    This also preserves compatibility with older test schemas.
    """

    allowed_tables = {
        "processed_events",
        "rejected_events",
        "duplicate_events"
    }

    if table_name not in allowed_tables:
        raise ValueError("Invalid event table")

    existing_columns = {
        row[1]
        for row in connection.execute(
            f"PRAGMA table_info({table_name})"
        ).fetchall()
    }

    row_values = dict(values)

    if "source_file" in existing_columns:
        row_values["source_file"] = source_file

    if "run_id" in existing_columns:
        row_values["run_id"] = run_id

    columns = list(row_values.keys())

    placeholders = ", ".join("?" for _ in columns)
    column_names = ", ".join(columns)

    sql = (
        f"INSERT INTO {table_name} "
        f"({column_names}) VALUES ({placeholders})"
    )

    connection.execute(
        sql,
        tuple(row_values[column] for column in columns)
    )


def save_batch(
    connection,
    source_file,
    accepted,
    rejected,
    duplicates,
    last_processed_offset,
    run_id=None
):
    """
    Save event outcomes and checkpoint atomically.

    If any insert fails, all writes in the batch are rolled back.
    """

    with connection:

        for event in accepted:
            insert_event_row(
                connection,
                "processed_events",
                {
                    "event_id": event["event_id"],
                    "order_id": event["order_id"],
                    "customer_id": event["customer_id"],
                    "event_type": event["event_type"],
                    "amount": event["amount"],
                    "event_time": event["event_time"],
                    "arrival_time": event["arrival_time"],
                    "is_late": int(event["is_late"]),
                    "source_offset": event["source_offset"]
                },
                source_file,
                run_id
            )

        for item in rejected:
            insert_event_row(
                connection,
                "rejected_events",
                {
                    "source_offset": item["offset"],
                    "raw_data": item["raw"],
                    "reason": item["reason"]
                },
                source_file,
                run_id
            )

        for item in duplicates:
            insert_event_row(
                connection,
                "duplicate_events",
                {
                    "source_offset": item["offset"],
                    "event_id": item["event_id"],
                    "reason": item["reason"]
                },
                source_file,
                run_id
            )

        connection.execute("""
            INSERT INTO checkpoints (
                source_file,
                last_processed_offset
            )
            VALUES (?, ?)
            ON CONFLICT(source_file)
            DO UPDATE SET
                last_processed_offset =
                    excluded.last_processed_offset
        """, (
            source_file,
            last_processed_offset
        ))


if __name__ == "__main__":
    initialize_database()
