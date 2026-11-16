
import json
import unittest

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

    def process_event(self, event, seen=None, max_time=None):
        batch = [{
            "offset": 1,
            "raw": json.dumps(event)
        }]

        return process_batch(
            batch,
            seen if seen is not None else set(),
            max_time
        )

    def test_valid_event(self):
        event = make_event("EVT001", "2026-11-16T10:00:00")

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 0)
        self.assertEqual(len(duplicates), 0)

    def test_duplicate_event(self):
        event = make_event("EVT001", "2026-11-16T10:00:00")

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
        event = make_event("EVT002", "2026-11-16T10:00:00", -100)

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)

    def test_late_event(self):
        first = make_event("EVT001", "2026-11-16T10:00:00")
        late = make_event("EVT002", "2026-11-16T09:00:00")

        batch = [
            {"offset": 1, "raw": json.dumps(first)},
            {"offset": 2, "raw": json.dumps(late)}
        ]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 2)
        self.assertEqual(sum(e["is_late"] for e in accepted), 1)

    def test_malformed_json(self):
        batch = [{"offset": 1, "raw": '{"event_id": BROKEN}'}]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["reason"], "Malformed JSON")

    def test_list_customer_id_is_rejected(self):
        event = make_event("EVT_LIST", "2026-11-16T10:00:00")
        event["customer_id"] = ["C001"]

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 0)
        self.assertEqual(rejected[0]["reason"], "Invalid customer reference")

    def test_dictionary_customer_id_is_rejected(self):
        event = make_event("EVT_DICT", "2026-11-16T10:00:00")
        event["customer_id"] = {"id": "C001"}

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 0)
        self.assertEqual(rejected[0]["reason"], "Invalid customer reference")

    def test_nan_amount_is_rejected(self):
        event = make_event("EVT_NAN", "2026-11-16T10:00:00", float("nan"))

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)

    def test_infinite_amount_is_rejected(self):
        event = make_event("EVT_INF", "2026-11-16T10:00:00", float("inf"))

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)

    def test_date_only_timestamp_is_rejected(self):
        event = make_event("EVT_DATE", "2026-11-16")

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)
        self.assertEqual(rejected[0]["reason"], "Invalid timestamp")

    def test_mixed_timezone_timestamps_do_not_crash(self):
        first = make_event("EVT_TZ1", "2026-11-16T10:00:00+05:30")
        second = make_event("EVT_TZ2", "2026-11-16T04:00:00")

        batch = [
            {"offset": 1, "raw": json.dumps(first)},
            {"offset": 2, "raw": json.dumps(second)}
        ]

        accepted, rejected, duplicates, max_time = process_batch(
            batch, set(), None
        )

        self.assertEqual(len(accepted), 2)
        self.assertEqual(len(rejected), 0)
        self.assertTrue(accepted[1]["is_late"])

    def test_timezone_is_normalized_to_utc(self):
        event = make_event("EVT_UTC", "2026-11-16T10:00:00+05:30")

        accepted, rejected, duplicates, max_time = self.process_event(event)

        self.assertEqual(len(accepted), 1)
        self.assertTrue(accepted[0]["event_time"].endswith("+00:00"))
        self.assertEqual(
            accepted[0]["event_time"],
            "2026-11-16T04:30:00+00:00"
        )


if __name__ == "__main__":
    unittest.main()
