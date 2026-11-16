
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import main

from storage.database import get_connection


class TestStreamPulseIntegration(unittest.TestCase):

    def setUp(self):
        self.temp_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_directory.name)

        self.input_file = self.root / "events.jsonl"
        self.database_file = self.root / "test.db"
        self.report_file = self.root / "report.json"

    def tearDown(self):
        self.temp_directory.cleanup()

    def write_events(self, count=2):
        with self.input_file.open("w", encoding="utf-8") as file:
            for number in range(1, count + 1):
                event = {
                    "event_id": f"EVT_{number:03d}",
                    "order_id": f"ORD_{number:03d}",
                    "customer_id": "C001",
                    "event_type": "ORDER_PLACED",
                    "amount": 500,
                    "event_time": f"2026-11-16T10:0{number}:00",
                    "arrival_time": f"2026-11-16T10:0{number}:05"
                }
                file.write(json.dumps(event) + "\n")

    def get_counts(self):
        connection = get_connection(self.database_file)
        try:
            return {
                table: connection.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0]
                for table in (
                    "processed_events",
                    "rejected_events",
                    "duplicate_events"
                )
            }
        finally:
            connection.close()

    def test_empty_input_completes_successfully(self):
        self.input_file.write_text("", encoding="utf-8")

        with patch("main.generate_report"):
            main.run_pipeline(
                self.input_file,
                2,
                self.database_file,
                self.report_file
            )

        connection = get_connection(self.database_file)
        try:
            run = connection.execute(
                "SELECT status FROM pipeline_runs"
            ).fetchone()

            self.assertEqual(run[0], "SUCCESS")

            checkpoint_count = connection.execute(
                "SELECT COUNT(*) FROM checkpoints"
            ).fetchone()[0]

            self.assertEqual(checkpoint_count, 0)
        finally:
            connection.close()

        self.assertEqual(
            self.get_counts(),
            {
                "processed_events": 0,
                "rejected_events": 0,
                "duplicate_events": 0
            }
        )

    def test_rerun_does_not_insert_duplicate_outcomes(self):
        self.write_events(2)

        with patch("main.generate_report"):
            main.run_pipeline(
                self.input_file, 1,
                self.database_file, self.report_file
            )

            first_counts = self.get_counts()

            main.run_pipeline(
                self.input_file, 1,
                self.database_file, self.report_file
            )

        second_counts = self.get_counts()

        self.assertEqual(first_counts, second_counts)
        self.assertEqual(second_counts["processed_events"], 2)

        connection = get_connection(self.database_file)
        try:
            checkpoint = connection.execute(
                "SELECT last_processed_offset FROM checkpoints"
            ).fetchone()[0]

            run_count = connection.execute(
                "SELECT COUNT(*) FROM pipeline_runs"
            ).fetchone()[0]

            self.assertEqual(checkpoint, 2)
            self.assertEqual(run_count, 2)
        finally:
            connection.close()

    def test_restart_resumes_after_simulated_failure(self):
        self.write_events(2)

        original_process_batch = main.process_batch
        call_count = 0

        def fail_on_second_batch(batch, seen_event_ids, max_event_time):
            nonlocal call_count
            call_count += 1

            if call_count == 2:
                raise RuntimeError("Simulated second-batch failure")

            return original_process_batch(
                batch,
                seen_event_ids,
                max_event_time
            )

        with patch("main.generate_report"):
            with patch(
                "main.process_batch",
                side_effect=fail_on_second_batch
            ):
                with self.assertRaisesRegex(
                    RuntimeError,
                    "Simulated second-batch failure"
                ):
                    main.run_pipeline(
                        self.input_file, 1,
                        self.database_file, self.report_file
                    )

            self.assertEqual(
                self.get_counts()["processed_events"],
                1
            )

            connection = get_connection(self.database_file)
            try:
                checkpoint = connection.execute(
                    "SELECT last_processed_offset FROM checkpoints"
                ).fetchone()[0]

                failed_status = connection.execute(
                    "SELECT status FROM pipeline_runs"
                ).fetchone()[0]

                self.assertEqual(checkpoint, 1)
                self.assertEqual(failed_status, "FAILED")
            finally:
                connection.close()

            main.run_pipeline(
                self.input_file, 1,
                self.database_file, self.report_file
            )

        self.assertEqual(
            self.get_counts()["processed_events"],
            2
        )

        connection = get_connection(self.database_file)
        try:
            checkpoint = connection.execute(
                "SELECT last_processed_offset FROM checkpoints"
            ).fetchone()[0]

            statuses = connection.execute("""
                SELECT status
                FROM pipeline_runs
                ORDER BY start_time
            """).fetchall()

            self.assertEqual(checkpoint, 2)
            self.assertEqual(
                [row[0] for row in statuses],
                ["FAILED", "SUCCESS"]
            )
        finally:
            connection.close()


if __name__ == "__main__":
    unittest.main()
