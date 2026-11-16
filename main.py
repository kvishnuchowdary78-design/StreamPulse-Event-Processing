
from pathlib import Path

from config import logger
from ingestion.event_ingestion import read_batches
from processing.event_processor import process_batch
from storage.database import (
    get_connection,
    initialize_database,
    get_last_processed_offset,
    get_processing_state,
    save_batch
)
from reports.aggregate_report import generate_report


INPUT_FILE = Path("data/input/events.jsonl")
SOURCE_FILE = str(INPUT_FILE)


def run_pipeline():
    initialize_database()

    logger.info("StreamPulse pipeline started")

    connection = get_connection()

    total_accepted = 0
    total_rejected = 0
    total_duplicates = 0
    total_late = 0
    batch_count = 0

    try:
        # Restore the last successfully processed offset
        last_offset = get_last_processed_offset(
            connection,
            SOURCE_FILE
        )

        # Restore previously processed event IDs and event-time state
        seen_event_ids, max_event_time = get_processing_state(
            connection
        )

        print("\n========== STREAMPULSE ==========")
        print("Starting offset:", last_offset)

        logger.info(
            "Starting from source offset %s",
            last_offset
        )

        # Read input records in batches of five
        batches = read_batches(
            INPUT_FILE,
            batch_size=5,
            start_offset=last_offset
        )

        for batch_number, batch in enumerate(batches, start=1):

            # Validate, deduplicate and detect late events
            accepted, rejected, duplicates, next_max_time = process_batch(
                batch,
                seen_event_ids,
                max_event_time
            )

            late_count = sum(
                1 for event in accepted
                if event["is_late"]
            )

            batch_last_offset = batch[-1]["offset"]

            # Store events and checkpoint atomically
            save_batch(
                connection,
                SOURCE_FILE,
                accepted,
                rejected,
                duplicates,
                batch_last_offset
            )

            max_event_time = next_max_time

            # Update processing metrics
            batch_count += 1
            total_accepted += len(accepted)
            total_rejected += len(rejected)
            total_duplicates += len(duplicates)
            total_late += late_count

            # Display batch results
            print(f"\nBatch {batch_number}")
            print("Last offset:", batch_last_offset)
            print("Accepted:", len(accepted))
            print("Rejected:", len(rejected))
            print("Duplicates:", len(duplicates))
            print("Late events:", late_count)
            print("Checkpoint saved:", batch_last_offset)

            # Record batch metrics in the log
            logger.info(
                "Batch completed | offset=%s | accepted=%s | rejected=%s | duplicates=%s | late=%s",
                batch_last_offset,
                len(accepted),
                len(rejected),
                len(duplicates),
                late_count
            )

        # Display pipeline summary
        print("\n========== PIPELINE SUMMARY ==========")
        print("Batches processed:", batch_count)
        print("Accepted:", total_accepted)
        print("Rejected:", total_rejected)
        print("Duplicates:", total_duplicates)
        print("Late events:", total_late)

        if batch_count == 0:
            print("No new records. Source is already up to date.")

        logger.info(
            "Pipeline completed | batches=%s | accepted=%s | rejected=%s | duplicates=%s | late=%s",
            batch_count,
            total_accepted,
            total_rejected,
            total_duplicates,
            total_late
        )

        # Generate aggregate report from the database
        print("\n========== REPORT GENERATION ==========")
        generate_report()

        logger.info("Aggregate report generated successfully")

    except Exception:
        logger.exception("StreamPulse pipeline failed")
        raise

    finally:
        connection.close()
        logger.info("Database connection closed")


if __name__ == "__main__":
    run_pipeline()
