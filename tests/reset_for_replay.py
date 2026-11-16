
import sqlite3

DATABASE = "data/output/streampulse.db"

def main():
    connection = sqlite3.connect(DATABASE)

    try:
        with connection:
            connection.execute("DELETE FROM processed_events")
            connection.execute("DELETE FROM rejected_events")
            connection.execute("DELETE FROM duplicate_events")
            connection.execute("DELETE FROM checkpoints")

        print("Replay preparation completed.")
        print("Working database tables cleared.")
        print("Backup remains unchanged.")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
