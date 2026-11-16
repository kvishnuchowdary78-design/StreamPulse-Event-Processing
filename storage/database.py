
import sqlite3
import json
from datetime import datetime
from pathlib import Path

DATABASE_FILE = Path("data/output/streampulse.db")


def get_connection():
    DATABASE_FILE.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DATABASE_FILE)
    connection.execute("PRAGMA foreign_keys = ON")
    return connection


def initialize_database():
    connection = get_connection()

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

    connection.close()
    print("StreamPulse database initialized successfully.")


def get_last_processed_offset(connection, source_file):
    result = connection.execute(
        """
        SELECT last_processed_offset
        FROM checkpoints
        WHERE source_file = ?
        """,
        (source_file,)
    ).fetchone()

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


def save_batch(
    connection,
    source_file,
    accepted,
    rejected,
    duplicates,
    last_processed_offset
):
    # All batch writes and checkpoint updates are one transaction.
    with connection:

        for event in accepted:
            connection.execute(
                """
                INSERT INTO processed_events (
                    event_id, order_id, customer_id, event_type,
                    amount, event_time, arrival_time, is_late,
                    source_offset
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["order_id"],
                    event["customer_id"],
                    event["event_type"],
                    event["amount"],
                    event["event_time"],
                    event["arrival_time"],
                    int(event["is_late"]),
                    event["source_offset"]
                )
            )

        for item in rejected:
            connection.execute(
                """
                INSERT INTO rejected_events (
                    source_offset, raw_data, reason
                )
                VALUES (?, ?, ?)
                """,
                (
                    item["offset"],
                    item["raw"],
                    item["reason"]
                )
            )

        for item in duplicates:
            connection.execute(
                """
                INSERT INTO duplicate_events (
                    source_offset, event_id, reason
                )
                VALUES (?, ?, ?)
                """,
                (
                    item["offset"],
                    item["event_id"],
                    item["reason"]
                )
            )

        connection.execute(
            """
            INSERT INTO checkpoints (
                source_file, last_processed_offset
            )
            VALUES (?, ?)
            ON CONFLICT(source_file)
            DO UPDATE SET
                last_processed_offset = excluded.last_processed_offset
            """,
            (source_file, last_processed_offset)
        )


if __name__ == "__main__":
    initialize_database()
