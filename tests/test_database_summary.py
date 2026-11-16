
import sqlite3


def main():
    database = "data/output/streampulse.db"

    connection = sqlite3.connect(database)

    tables = {
        "processed_events": "Accepted events",
        "rejected_events": "Rejected events",
        "duplicate_events": "Duplicate events"
    }

    print("\n========== DATABASE AUDIT ==========")

    for table, description in tables.items():
        count = connection.execute(
            f"SELECT COUNT(*) FROM {table}"
        ).fetchone()[0]

        print(f"{description}: {count}")

    checkpoint = connection.execute(
        """
        SELECT source_file, last_processed_offset
        FROM checkpoints
        """
    ).fetchall()

    print("\nCheckpoint records:")

    for source_file, offset in checkpoint:
        print("Source:", source_file)
        print("Last processed offset:", offset)

    connection.close()


if __name__ == "__main__":
    main()
