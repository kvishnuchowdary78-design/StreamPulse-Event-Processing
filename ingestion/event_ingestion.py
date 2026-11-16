
import json
from pathlib import Path

CHECKPOINT_FILE = Path("checkpoints/source_checkpoint.json")


def get_last_processed_offset():
    if not CHECKPOINT_FILE.exists():
        return 0

    with open(CHECKPOINT_FILE, "r", encoding="utf-8") as file:
        checkpoint = json.load(file)

    return checkpoint["last_processed_offset"]


def read_batches(input_file, batch_size=5, start_offset=None):
    if start_offset is None:
        start_offset = get_last_processed_offset()

    batch = []

    with open(input_file, "r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):

            if line_number <= start_offset:
                continue

            record = {
                "offset": line_number,
                "raw": line.strip()
            }

            batch.append(record)

            if len(batch) == batch_size:
                yield batch
                batch = []

    if batch:
        yield batch


def save_checkpoint(last_processed_offset):
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)

    checkpoint = {
        "source_file": "data/input/events.jsonl",
        "last_processed_offset": last_processed_offset
    }

    temporary_file = CHECKPOINT_FILE.with_suffix(".tmp")

    with open(temporary_file, "w", encoding="utf-8") as file:
        json.dump(checkpoint, file, indent=4)

    temporary_file.replace(CHECKPOINT_FILE)


if __name__ == "__main__":
    batches = read_batches(
        "data/input/events.jsonl",
        batch_size=5,
        start_offset=0
    )

    for batch_number, batch in enumerate(batches, start=1):
        print(f"\nBatch {batch_number}")

        for record in batch:
            print("Offset:", record["offset"])
