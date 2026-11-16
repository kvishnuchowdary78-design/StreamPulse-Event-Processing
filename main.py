import argparse
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


DEFAULT_INPUT_FILE = Path("data/input/events.jsonl")
DEFAULT_BATCH_SIZE = 5
DEFAULT_DATABASE = Path("data/output/streampulse.db")
DEFAULT_REPORT = Path("reports/aggregate_summary.json")


def run_pipeline(
    input_file=DEFAULT_INPUT_FILE,
    batch_size=DEFAULT_BATCH_SIZE,
    database_file=DEFAULT_DATABASE,
    report_file=DEFAULT_REPORT
):
    input_file = Path(input_file)
    database_file = Path(database_file)
    report_file = Path(report_file)
    source_file = str(input_file)

    if batch_size < 1:
        raise ValueError("Batch size must be at least 1")

    if not input_file.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")

    initialize_database(database_file)
    logger.info("StreamPulse pipeline started")

    connection = get_connection(database_file)

    total_accepted = 0
    total_rejected = 0
    total_duplicates = 0
    total_late = 0
    batch_count = 0

    try:
        last_offset = get_last_processed_offset(
            connection,
            source_file
        )

        seen_event_ids, max_event_time = get_processing_state(
            connection
        )

        print("\n========== STREAMPULSE ==========")
        print("Input file:", input_file)
        print("Database:", database_file)
        print("Report:", report_file)
        print("Batch size:", batch_size)
        print("Starting offset:", last_offset)

        logger.info("Starting from source offset %s", last_offset)

        batches = read_batches(
            input_file,
            batch_size=batch_size,
            start_offset=last_offset
        )

        for batch_number, batch in enumerate(batches, start=1):
            accepted, rejected, duplicates, next_max_time = process_batch(
                batch,
                seen_event_ids,
                max_event_time
            )

            late_count = sum(
                1 for event in accepted if event["is_late"]
            )

            batch_last_offset = batch[-1]["offset"]

            save_batch(
                connection,
                source_file,
                accepted,
                rejected,
                duplicates,
                batch_last_offset
            )

            max_event_time = next_max_time

            batch_count += 1
            total_accepted += len(accepted)
            total_rejected += len(rejected)
            total_duplicates += len(duplicates)
            total_late += late_count

            print(f"\nBatch {batch_number}")
            print("Last offset:", batch_last_offset)
            print("Accepted:", len(accepted))
            print("Rejected:", len(rejected))
            print("Duplicates:", len(duplicates))
            print("Late events:", late_count)
            print("Checkpoint saved:", batch_last_offset)

            logger.info(
                "Batch completed | offset=%s | accepted=%s | rejected=%s | duplicates=%s | late=%s",
                batch_last_offset,
                len(accepted),
                len(rejected),
                len(duplicates),
                late_count
            )

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

        print("\n========== REPORT GENERATION ==========")
        generate_report(database_file, report_file)

        logger.info("Aggregate report generated successfully")

    except Exception:
        logger.exception("StreamPulse pipeline failed")
        raise

    finally:
        connection.close()
        logger.info("Database connection closed")


def main():
    parser = argparse.ArgumentParser(
        description="StreamPulse configurable event processing pipeline."
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT_FILE,
        help="Input JSONL file."
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help="Number of records per batch."
    )

    parser.add_argument(
        "--database",
        type=Path,
        default=DEFAULT_DATABASE,
        help="SQLite database file."
    )

    parser.add_argument(
        "--report",
        type=Path,
        default=DEFAULT_REPORT,
        help="Aggregate report output file."
    )

    args = parser.parse_args()

    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")

    run_pipeline(
        input_file=args.input,
        batch_size=args.batch_size,
        database_file=args.database,
        report_file=args.report
    )


if __name__ == "__main__":
    main()
