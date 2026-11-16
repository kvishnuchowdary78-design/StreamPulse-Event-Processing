
import json
import math
from datetime import datetime, timezone


VALID_CUSTOMERS = {
    "C001", "C002", "C003", "C004", "C005"
}

VALID_EVENT_TYPES = {"ORDER_PLACED"}


def parse_timestamp(value):
    """Parse an ISO timestamp and normalize it to UTC."""

    if not isinstance(value, str) or "T" not in value:
        raise ValueError("Timestamp must contain a date and time")

    normalized = value.strip()

    if normalized.endswith("Z"):
        normalized = normalized[:-1] + "+00:00"

    parsed = datetime.fromisoformat(normalized)

    # Treat timestamps without timezone information as UTC.
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def normalize_datetime(value):
    """Make an existing datetime safe for UTC comparisons."""

    if value is None:
        return None

    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)

    return value.astimezone(timezone.utc)


def validate_event(event):
    """Check whether an event satisfies business rules."""

    required_fields = [
        "event_id",
        "order_id",
        "customer_id",
        "event_type",
        "amount",
        "event_time",
        "arrival_time"
    ]

    for field in required_fields:
        if field not in event or event[field] is None:
            return False, f"Missing required field: {field}"

    if not isinstance(event["event_id"], str) or not event["event_id"].strip():
        return False, "Invalid event_id"

    if not isinstance(event["order_id"], str) or not event["order_id"].strip():
        return False, "Invalid order_id"

    # Check the type before set membership to avoid unhashable-value crashes.
    if (
        not isinstance(event["customer_id"], str)
        or event["customer_id"] not in VALID_CUSTOMERS
    ):
        return False, "Invalid customer reference"

    if (
        not isinstance(event["event_type"], str)
        or event["event_type"] not in VALID_EVENT_TYPES
    ):
        return False, "Invalid event_type"

    amount = event["amount"]

    if (
        isinstance(amount, bool)
        or not isinstance(amount, (int, float))
        or not math.isfinite(amount)
        or amount <= 0
    ):
        return False, "Amount must be a finite positive number"

    try:
        event_time = parse_timestamp(event["event_time"])
        arrival_time = parse_timestamp(event["arrival_time"])
    except (ValueError, TypeError, OverflowError):
        return False, "Invalid timestamp"

    # Store normalized timestamps so database state remains consistent.
    event["event_time"] = event_time.isoformat()
    event["arrival_time"] = arrival_time.isoformat()

    return True, event_time


def process_batch(batch, seen_event_ids, max_event_time):
    """Parse, validate, deduplicate and identify late events."""

    accepted = []
    rejected = []
    duplicates = []

    max_event_time = normalize_datetime(max_event_time)

    for record in batch:

        offset = record["offset"]
        raw_data = record["raw"]

        # Step 1: Parse JSON.
        try:
            event = json.loads(raw_data)
        except (json.JSONDecodeError, TypeError):
            rejected.append({
                "offset": offset,
                "raw": raw_data,
                "reason": "Malformed JSON"
            })
            continue

        if not isinstance(event, dict):
            rejected.append({
                "offset": offset,
                "raw": raw_data,
                "reason": "Expected a JSON object"
            })
            continue

        # Step 2: Validate business rules.
        is_valid, result = validate_event(event)

        if not is_valid:
            rejected.append({
                "offset": offset,
                "raw": raw_data,
                "reason": result
            })
            continue

        event_time = result

        # Step 3: Detect duplicate event IDs.
        event_id = event["event_id"]

        if event_id in seen_event_ids:
            duplicates.append({
                "offset": offset,
                "event_id": event_id,
                "reason": "Duplicate event ID"
            })
            continue

        # Step 4: Identify late/out-of-order events.
        is_late = (
            max_event_time is not None
            and event_time < max_event_time
        )

        if max_event_time is None or event_time > max_event_time:
            max_event_time = event_time

        event["is_late"] = is_late
        event["source_offset"] = offset

        seen_event_ids.add(event_id)
        accepted.append(event)

    return accepted, rejected, duplicates, max_event_time


if __name__ == "__main__":
    print("StreamPulse Event Processor loaded successfully.")
