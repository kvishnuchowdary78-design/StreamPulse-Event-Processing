
import unittest
from datetime import datetime

from processing.event_processor import process_batch


def make_event(event_id, event_time, amount=500):
    return {
        "event_id": event_id,
        "order_id": "ORD001",
        "customer_id": "C001",
        "event_type": "ORDER_PLACED",
        "amount": amount,
        "event_time": event_time,
        "arrival_time": "2026-11-16T10:05:00"
    }


class TestStreamPulseProcessor(unittest.TestCase):

    def test_valid_event(self):
        event = make_event(
            "EVT001",
            "2026-11-16T10:00:00"
        )

        batch = [{"offset": 1, "raw": __import__("json").dumps(event)}]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 0)
        self.assertEqual(len(duplicates), 0)

    def test_duplicate_event(self):
        event = make_event(
            "EVT001",
            "2026-11-16T10:00:00"
        )

        import json

        batch = [
            {"offset": 1, "raw": json.dumps(event)},
            {"offset": 2, "raw": json.dumps(event)}
        ]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(duplicates), 1)

    def test_invalid_amount(self):
        event = make_event(
            "EVT002",
            "2026-11-16T10:00:00",
            amount=-100
        )

        import json

        batch = [{"offset": 1, "raw": json.dumps(event)}]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)

    def test_late_event(self):
        import json

        first_event = make_event(
            "EVT001",
            "2026-11-16T10:00:00"
        )

        late_event = make_event(
            "EVT002",
            "2026-11-16T09:00:00"
        )

        batch = [
            {"offset": 1, "raw": json.dumps(first_event)},
            {"offset": 2, "raw": json.dumps(late_event)}
        ]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        late_count = sum(
            1 for event in accepted if event["is_late"]
        )

        self.assertEqual(len(accepted), 2)
        self.assertEqual(late_count, 1)

    def test_malformed_json(self):
        batch = [
            {"offset": 1, "raw": '{"event_id": BROKEN}'}
        ]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["reason"], "Malformed JSON")


if __name__ == "__main__":
    unittest.main()
