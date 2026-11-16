import argparse
import json
import sqlite3
from pathlib import Path
from math import isclose


DEFAULT_INPUT = Path("data/input/events.jsonl")
DEFAULT_DATABASE = Path("data/output/streampulse.db")
DEFAULT_REPORT = Path("reports/aggregate_summary.json")
DEFAULT_OUTPUT = Path("reports/reconciliation_summary.json")


def reconcile(input_file, database_file, report_file, output_file):
    input_file = Path(input_file)
    database_file = Path(database_file)
    report_file = Path(report_file)
    output_file = Path(output_file)

    for path in (input_file, database_file, report_file):
        if not path.exists():
            raise FileNotFoundError(f"Required file not found: {path}")

    source_file = str(input_file)

    with input_file.open("r", encoding="utf-8") as file:
        source_records = sum(1 for _ in file)

    with sqlite3.connect(database_file) as connection:
        accepted = connection.execute(
            "SELECT COUNT(*) FROM processed_events"
        ).fetchone()[0]

        rejected = connection.execute(
            "SELECT COUNT(*) FROM rejected_events"
        ).fetchone()[0]

        duplicates = connection.execute(
            "SELECT COUNT(*) FROM duplicate_events"
        ).fetchone()[0]

        revenue, late_events = connection.execute("""
            SELECT
                COALESCE(SUM(amount), 0),
                COALESCE(SUM(is_late), 0)
            FROM processed_events
        """).fetchone()

        checkpoint_row = connection.execute(
            """
            SELECT last_processed_offset
            FROM checkpoints
            WHERE source_file = ?
            """,
            (source_file,)
        ).fetchone()

        checkpoint = checkpoint_row[0] if checkpoint_row else 0

        offsets = connection.execute("""
            SELECT source_offset FROM processed_events
            UNION ALL
            SELECT source_offset FROM rejected_events
            UNION ALL
            SELECT source_offset FROM duplicate_events
            ORDER BY source_offset
        """).fetchall()

    recorded_offsets = [row[0] for row in offsets]
    total_outcomes = accepted + rejected + duplicates

    unique_offsets = len(set(recorded_offsets))

    continuous_offsets = (
        recorded_offsets == list(range(1, checkpoint + 1))
    )

    with report_file.open("r", encoding="utf-8") as file:
        report = json.load(file)

    overall = report["overall"]
    quality = report["data_quality"]

    checks = {
        "Checkpoint does not exceed the number of source records":
            checkpoint <= source_records,

        "Every checkpointed source record has one recorded outcome":
            total_outcomes == checkpoint,

        "Accepted + rejected + duplicates equals checkpoint":
            accepted + rejected + duplicates == checkpoint,

        "No source offset appears in more than one outcome":
            len(recorded_offsets) == unique_offsets,

        "Processed source offsets are continuous from 1 to checkpoint":
            continuous_offsets,

        "Aggregate report matches database totals":
            (
                overall["total_orders"] == accepted
                and quality["rejected_events"] == rejected
                and quality["duplicate_events"] == duplicates
                and overall["late_orders"] == late_events
                and isclose(
                    float(overall["total_revenue"]),
                    float(revenue),
                    rel_tol=1e-9,
                    abs_tol=0.01
                )
            )
    }

    print("\n========== STREAMPULSE RECONCILIATION ==========\n")
    print("Input:", input_file)
    print("Database:", database_file)
    print("Source records:", source_records)
    print("Checkpoint:", checkpoint)
    print("Accepted:", accepted)
    print("Rejected:", rejected)
    print("Duplicates:", duplicates)
    print("Total recorded outcomes:", total_outcomes)
    print(f"Accepted revenue: {revenue:.2f}")
    print("Late events:", late_events)

    for description, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        print(f"[{status}] {description}")

    passed_count = sum(checks.values())
    pending = max(source_records - checkpoint, 0)

    result = {
        "input_file": str(input_file),
        "database_file": str(database_file),
        "source_records": source_records,
        "checkpoint": checkpoint,
        "accepted": accepted,
        "rejected": rejected,
        "duplicates": duplicates,
        "total_outcomes": total_outcomes,
        "accepted_revenue": revenue,
        "late_events": late_events,
        "pending_records": pending,
        "checks": checks,
        "checks_passed": passed_count,
        "checks_total": len(checks),
        "status": "PASS" if passed_count == len(checks) else "FAIL"
    }

    output_file.parent.mkdir(parents=True, exist_ok=True)

    with output_file.open("w", encoding="utf-8") as file:
        json.dump(result, file, indent=4)

    print("\n========== RECONCILIATION RESULT ==========")
    print(f"Checks passed: {passed_count}/{len(checks)}")
    print("Pending source records:", pending)
    print("Status:", result["status"])
    print("Report saved to:", output_file)

    if result["status"] != "PASS":
        raise SystemExit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Reconcile StreamPulse source records, database, and report."
    )

    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)

    args = parser.parse_args()

    reconcile(
        args.input,
        args.database,
        args.report,
        args.output
    )


if __name__ == "__main__":
    main()
