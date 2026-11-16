import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path


DEFAULT_OUTPUT = Path("data/input/events.jsonl")


def generate_events(count=20, seed=42, include_edge_cases=True):
    """Generate reproducible synthetic e-commerce events."""

    if count < 5 and include_edge_cases:
        raise ValueError("Use at least 5 normal events when edge cases are enabled.")

    rng = random.Random(seed)
    events = []
    base_time = datetime(2026, 11, 16, 9, 0, 0)

    for i in range(1, count + 1):
        event_time = base_time + timedelta(minutes=i * 2)

        events.append({
            "event_id": f"EVT{i:06d}",
            "order_id": f"ORD{i:06d}",
            "customer_id": f"C{rng.randint(1, 5):03d}",
            "event_type": "ORDER_PLACED",
            "amount": rng.randint(100, 5000),
            "event_time": event_time.isoformat(),
            "arrival_time": (
                event_time + timedelta(seconds=5)
            ).isoformat()
        })

    if include_edge_cases:
        next_id = count + 1

        # Duplicate: reuse an existing event ID.
        events.append(events[2].copy())

        # Missing customer reference.
        missing_customer = events[0].copy()
        missing_customer["event_id"] = f"EVT{next_id:06d}"
        missing_customer["order_id"] = f"ORD{next_id:06d}"
        missing_customer["customer_id"] = None
        events.append(missing_customer)
        next_id += 1

        # Invalid transaction amount.
        invalid_amount = events[1].copy()
        invalid_amount["event_id"] = f"EVT{next_id:06d}"
        invalid_amount["order_id"] = f"ORD{next_id:06d}"
        invalid_amount["amount"] = -500
        events.append(invalid_amount)
        next_id += 1

        # Late-arriving event.
        late_event = events[3].copy()
        late_event["event_id"] = f"EVT{next_id:06d}"
        late_event["order_id"] = f"ORD{next_id:06d}"
        late_event["event_time"] = (
            base_time - timedelta(hours=2)
        ).isoformat()
        late_event["arrival_time"] = (
            base_time + timedelta(hours=1)
        ).isoformat()
        events.append(late_event)
        next_id += 1

        # Out-of-order event.
        out_of_order = events[4].copy()
        out_of_order["event_id"] = f"EVT{next_id:06d}"
        out_of_order["order_id"] = f"ORD{next_id:06d}"
        out_of_order["event_time"] = (
            base_time - timedelta(minutes=30)
        ).isoformat()
        events.append(out_of_order)

    return events


def save_events(events, output_file):
    """Write JSON Lines and one malformed record for validation testing."""

    output_file = Path(output_file)
    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        for event in events:
            file.write(json.dumps(event) + "\n")

        file.write('{"event_id": "BROKEN_JSON", "amount": }\n')

    print(f"Generated {len(events)} JSON events.")
    print("Additional malformed record: 1")
    print(f"Total source records: {len(events) + 1}")
    print(f"Saved dataset to: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="Generate reproducible StreamPulse event data."
    )
    parser.add_argument(
        "--count",
        type=int,
        default=20,
        help="Number of normal events (default: 20)."
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data (default: 42)."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Output JSONL file path."
    )
    parser.add_argument(
        "--no-edge-cases",
        action="store_true",
        help="Do not append duplicate and invalid test events."
    )

    args = parser.parse_args()

    if args.count < 1:
        parser.error("--count must be at least 1.")

    try:
        events = generate_events(
            count=args.count,
            seed=args.seed,
            include_edge_cases=not args.no_edge_cases
        )
        save_events(events, args.output)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
