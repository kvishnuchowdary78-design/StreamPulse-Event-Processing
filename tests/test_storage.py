
import sqlite3
import unittest

from storage.database import save_batch


class TestStreamPulseStorage(unittest.TestCase):

    def setUp(self):
        self.connection = sqlite3.connect(":memory:")

        self.connection.executescript("""
            CREATE TABLE processed_events (
                event_id TEXT PRIMARY KEY,
                order_id TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                amount REAL NOT NULL,
                event_time TEXT NOT NULL,
                arrival_time TEXT NOT NULL,
                is_late INTEGER NOT NULL,
                source_offset INTEGER UNIQUE NOT NULL
            );

            CREATE TABLE rejected_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_offset INTEGER UNIQUE NOT NULL,
                raw_data TEXT NOT NULL,
                reason TEXT NOT NULL
            );

            CREATE TABLE duplicate_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_offset INTEGER UNIQUE NOT NULL,
                event_id TEXT NOT NULL,
                reason TEXT NOT NULL
            );

            CREATE TABLE checkpoints (
                source_file TEXT PRIMARY KEY,
                last_processed_offset INTEGER NOT NULL
            );
        """)

        # Create a conflicting offset to force a database error.
        self.connection.execute("""
            INSERT INTO rejected_events
            (source_offset, raw_data, reason)
            VALUES (99, 'existing record', 'test conflict')
        """)
        self.connection.commit()

    def tearDown(self):
        self.connection.close()

    def test_failed_batch_rolls_back_all_writes(self):
        accepted = [{
            "event_id": "EVT_TEST",
            "order_id": "ORD_TEST",
            "customer_id": "C001",
            "event_type": "ORDER_PLACED",
            "amount": 500,
            "event_time": "2026-11-16T10:00:00",
            "arrival_time": "2026-11-16T10:00:05",
            "is_late": False,
            "source_offset": 1
        }]

        rejected = [{
            "offset": 99,
            "raw": "invalid test record",
            "reason": "Test conflict"
        }]

        with self.assertRaises(sqlite3.IntegrityError):
            save_batch(
                self.connection,
                "test_events.jsonl",
                accepted,
                rejected,
                [],
                99
            )

        accepted_count = self.connection.execute(
            "SELECT COUNT(*) FROM processed_events"
        ).fetchone()[0]

        checkpoint_count = self.connection.execute(
            "SELECT COUNT(*) FROM checkpoints"
        ).fetchone()[0]

        self.assertEqual(accepted_count, 0)
        self.assertEqual(checkpoint_count, 0)


if __name__ == "__main__":
    unittest.main()
