
import sqlite3
import tempfile
import unittest

from pathlib import Path

from storage.database import (
    save_batch,
    get_connection,
    initialize_database,
    create_pipeline_run,
    finish_pipeline_run
)


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


class TestPipelineRunTracking(unittest.TestCase):

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()

        self.database_file = (
            Path(self.temp_directory.name) / "test.db"
        )

        initialize_database(self.database_file)

        self.connection = get_connection(
            self.database_file
        )

    def tearDown(self):
        self.connection.close()
        self.temp_directory.cleanup()

    def test_pipeline_run_success(self):

        run_id = create_pipeline_run(
            self.connection,
            "events.jsonl"
        )

        finish_pipeline_run(
            self.connection,
            run_id,
            "SUCCESS"
        )

        row = self.connection.execute("""
            SELECT status, end_time, failure_reason
            FROM pipeline_runs
            WHERE run_id = ?
        """, (run_id,)).fetchone()

        self.assertEqual(row[0], "SUCCESS")
        self.assertIsNotNone(row[1])
        self.assertIsNone(row[2])

    def test_pipeline_run_failure(self):

        run_id = create_pipeline_run(
            self.connection,
            "events.jsonl"
        )

        finish_pipeline_run(
            self.connection,
            run_id,
            "FAILED",
            "Simulated processing failure"
        )

        row = self.connection.execute("""
            SELECT status, end_time, failure_reason
            FROM pipeline_runs
            WHERE run_id = ?
        """, (run_id,)).fetchone()

        self.assertEqual(row[0], "FAILED")
        self.assertIsNotNone(row[1])
        self.assertEqual(
            row[2],
            "Simulated processing failure"
        )

    def test_event_outcomes_store_run_metadata(self):

        run_id = create_pipeline_run(
            self.connection,
            "events.jsonl"
        )

        accepted = [{
            "event_id": "EVT_001",
            "order_id": "ORD_001",
            "customer_id": "C001",
            "event_type": "ORDER_PLACED",
            "amount": 500,
            "event_time": "2026-11-16T10:00:00",
            "arrival_time": "2026-11-16T10:00:05",
            "is_late": False,
            "source_offset": 1
        }]

        rejected = [{
            "offset": 2,
            "raw": '{"invalid": true}',
            "reason": "Invalid test event"
        }]

        duplicates = [{
            "offset": 3,
            "event_id": "EVT_DUPLICATE",
            "reason": "Duplicate event"
        }]

        save_batch(
            self.connection,
            "events.jsonl",
            accepted,
            rejected,
            duplicates,
            3,
            run_id=run_id
        )

        for table_name in (
            "processed_events",
            "rejected_events",
            "duplicate_events"
        ):
            row = self.connection.execute(
                f"SELECT run_id, source_file FROM {table_name}"
            ).fetchone()

            self.assertEqual(row[0], run_id)
            self.assertEqual(row[1], "events.jsonl")


if __name__ == "__main__":
    unittest.main()
