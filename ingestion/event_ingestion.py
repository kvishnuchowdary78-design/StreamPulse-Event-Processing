from pathlib import Path


def read_batches(input_file, batch_size=5, start_offset=0):
    """Read JSONL input records in batches, resuming after a source offset."""

    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    if start_offset < 0:
        raise ValueError("start_offset cannot be negative")

    input_file = Path(input_file)

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    batch = []

    with input_file.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):

            if line_number <= start_offset:
                continue

            batch.append({
                "offset": line_number,
                "raw": line.strip()
            })

            if len(batch) >= batch_size:
                yield batch
                batch = []

    if batch:
        yield batch


if __name__ == "__main__":
    for batch_number, batch in enumerate(
        read_batches("data/input/events.jsonl", batch_size=5),
        start=1
    ):
        print(f"\nBatch {batch_number}")
        for record in batch:
            print("Offset:", record["offset"])
