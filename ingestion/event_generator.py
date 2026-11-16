
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


# Fixed seed ensures reproducible data
random.seed(42)

OUTPUT_FILE = Path("data/input/events.jsonl")


def generate_events(count=20):
    """Generate synthetic e-commerce order events."""

    events = []

    base_time = datetime(2026, 11, 16, 9, 0, 0)

    for i in range(1, count + 1):

        event_time = base_time + timedelta(minutes=i * 2)

        event = {
            "event_id": f"EVT{i:04d}",
            "order_id": f"ORD{i:04d}",
            "customer_id": f"C{random.randint(1, 5):03d}",
            "event_type": "ORDER_PLACED",
            "amount": random.randint(100, 5000),
            "event_time": event_time.isoformat(),
            "arrival_time": (event_time + timedelta(seconds=5)).isoformat()
        }

        events.append(event)

    # Edge case 1: Duplicate event
    events.append(events[2].copy())

    # Edge case 2: Missing customer ID
    missing_customer = events[0].copy()
    missing_customer["event_id"] = "EVT_INVALID_01"
    missing_customer["order_id"] = "ORD_INVALID_01"
    missing_customer["customer_id"] = None
    events.append(missing_customer)

    # Edge case 3: Negative transaction amount
    invalid_amount = events[1].copy()
    invalid_amount["event_id"] = "EVT_INVALID_02"
    invalid_amount["order_id"] = "ORD_INVALID_02"
    invalid_amount["amount"] = -500
    events.append(invalid_amount)

    # Edge case 4: Late-arriving event
    late_event = events[3].copy()
    late_event["event_id"] = "EVT_LATE_01"
    late_event["order_id"] = "ORD_LATE_01"
    late_event["event_time"] = (
        base_time - timedelta(hours=2)
    ).isoformat()
    late_event["arrival_time"] = (
        base_time + timedelta(hours=1)
    ).isoformat()
    events.append(late_event)

    # Edge case 5: Out-of-order event
    out_of_order = events[4].copy()
    out_of_order["event_id"] = "EVT_LATE_02"
    out_of_order["order_id"] = "ORD_LATE_02"
    out_of_order["event_time"] = (
        base_time - timedelta(minutes=30)
    ).isoformat()
    events.append(out_of_order)

    return events


def save_events(events):
    """Save events as JSON Lines."""

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(OUTPUT_FILE, "w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

        # Deliberately malformed JSON for testing
        file.write('{"event_id": "BROKEN_JSON", "amount": }\n')

    print(f"Generated {len(events)} JSON events.")
    print(f"Saved dataset to: {OUTPUT_FILE}")
    print("One additional malformed JSON line was added.")


def main():
    events = generate_events(20)
    save_events(events)


if __name__ == "__main__":
    main()
