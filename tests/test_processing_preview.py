
from ingestion.event_ingestion import read_batches
from processing.event_processor import process_batch


def main():

    input_file = "data/input/events.jsonl"

    seen_event_ids = set()
    max_event_time = None

    total_accepted = 0
    total_rejected = 0
    total_duplicates = 0
    total_late = 0

    batches = read_batches(input_file, batch_size=5)

    for batch_number, batch in enumerate(batches, start=1):

        accepted, rejected, duplicates, max_event_time = (
            process_batch(
                batch,
                seen_event_ids,
                max_event_time
            )
        )

        total_accepted += len(accepted)
        total_rejected += len(rejected)
        total_duplicates += len(duplicates)

        late_count = sum(
            1 for event in accepted if event["is_late"]
        )

        total_late += late_count

        print(f"\nBatch {batch_number}")
        print("Accepted:", len(accepted))
        print("Rejected:", len(rejected))
        print("Duplicates:", len(duplicates))
        print("Late events:", late_count)

        for item in rejected:
            print("Rejected reason:", item["reason"])

    print("\n========== FINAL SUMMARY ==========")
    print("Total accepted:", total_accepted)
    print("Total rejected:", total_rejected)
    print("Total duplicates:", total_duplicates)
    print("Total late events:", total_late)


if __name__ == "__main__":
    main()
