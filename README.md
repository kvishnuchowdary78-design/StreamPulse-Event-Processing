# StreamPulse - Micro-Batch Event Processing and Monitoring System

## 1. Project Overview

StreamPulse is a Python-based micro-batch event processing system that ingests JSONL event data, validates records, detects duplicates and late-arriving events, stores processing outcomes in SQLite, and generates aggregate reports.

The project demonstrates practical data engineering concepts, including batch processing, data validation, checkpoint-based recovery, transactional persistence, pipeline execution tracking, and independent reconciliation.

## 2. Key Features

- **JSONL ingestion:** Reads event records from newline-delimited JSON files.
- **Micro-batch processing:** Processes events in configurable batches.
- **Data validation:** Detects malformed records, invalid amounts, missing or invalid fields, and invalid timestamps.
- **Duplicate detection:** Identifies duplicate events.
- **Late-event detection:** Identifies events arriving out of chronological order.
- **SQLite persistence:** Stores accepted, rejected, and duplicate event outcomes.
- **Checkpointing:** Tracks source offsets to support restart and recovery.
- **Transactional batch writes:** Saves batch outcomes and checkpoint updates atomically.
- **Aggregate reporting:** Calculates event counts, revenue, average order value, and customer-level metrics.
- **Pipeline run tracking:** Records unique run IDs, source files, execution timestamps, status, and failure reasons.
- **Event-level lineage:** Associates newly processed event outcomes with their pipeline run IDs and source files.
- **Independent reconciliation:** Compares source records, database outcomes, checkpoints, and generated reports.
- **Automated testing:** Includes processor, storage, transaction, and pipeline run-tracking tests.

## 3. Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python 3.10 |
| Database | SQLite |
| Input format | JSONL |
| Data processing | Python |
| Reporting | JSON |
| Testing | Python unittest |
| Version control | Git and GitHub |

## 4. Architecture

The pipeline follows four main stages:

### 4.1 Ingestion

- Reads JSONL records from the input file.
- Maintains source offsets.
- Supplies records to the processing layer in configurable batches.

### 4.2 Processing

- Parses and validates event records.
- Checks event identifiers and timestamps.
- Detects duplicate and late events.
- Separates accepted and rejected records.

### 4.3 Storage

- Persists processing outcomes in SQLite.
- Maintains checkpoint information.
- Uses database transactions to keep batch outcomes and checkpoint updates consistent.
- Records pipeline execution metadata in the `pipeline_runs` table.

### 4.4 Reporting and Reconciliation

- Generates aggregate metrics.
- Produces customer-level summaries.
- Independently reconciles source records against persisted outcomes and checkpoint information.

## 5. Project Structure

```text
StreamPulse/
|
+-- data/
|   +-- input/
|   |   +-- events.jsonl
|   |   +-- benchmark_events.jsonl
|   |
|   +-- output/
|
+-- ingestion/
|   +-- event_generator.py
|   +-- event_ingestion.py
|
+-- processing/
|   +-- event_processor.py
|
+-- storage/
|   +-- database.py
|
+-- reports/
|   +-- aggregate_report.py
|   +-- aggregate_summary.json
|   +-- reconciliation.py
|   +-- reconciliation_summary.json
|   +-- benchmark_summary.json
|   +-- benchmark_reconciliation.json
|
+-- tests/
|   +-- test_pipeline.py
|   +-- test_storage.py
|
+-- main.py
+-- README.md
+-- DESIGN_NOTES.md
+-- DATA_DICTIONARY.md
```

## 6. Setup and Execution

### Prerequisites

- Python 3.10 or later
- Git

### Run the project

Clone the repository:

```bash
git clone https://github.com/kvishnuchowdary78-design/StreamPulse-Event-Processing.git
cd StreamPulse-Event-Processing
```

Run the pipeline using the default input:

```bash
python main.py
```

Run with custom parameters:

```bash
python main.py --input data/input/events.jsonl --batch-size 5 --database data/output/events_fresh.db --report reports/aggregate_summary.json
python reports/reconciliation.py --input data/input/events.jsonl --database data/output/events_fresh.db --report reports/aggregate_summary.json --output reports/reconciliation_summary.json
```

### Supported command-line arguments

| Argument | Description |
|---|---|
| `--input` | Input JSONL file |
| `--batch-size` | Number of records per batch |
| `--database` | SQLite database path |
| `--report` | Aggregate report output path |

## 7. Pipeline Run Tracking

Every pipeline execution that passes initial input validation receives a unique UUID-based run ID.

The `pipeline_runs` table stores:

- `run_id`
- `source_file`
- `start_time`
- `end_time`
- `status`
- `failure_reason`

A run begins with `RUNNING` status and transitions to:

- `SUCCESS` after processing and report generation complete.
- `FAILED` when an exception interrupts execution.

Execution timestamps are stored in UTC.

The event outcome tables also include `source_file` and `run_id` metadata for newly processed records.

This makes it possible to inspect pipeline execution history and associate event outcomes with individual runs.

## 8. Benchmark Results

The benchmark dataset contains 1,006 source records, including normal records and deliberately introduced edge cases.

Verified benchmark results:

| Metric | Result |
|---|---:|
| Source records | 1,006 |
| Checkpoint | 1,006 |
| Accepted events | 1,002 |
| Rejected events | 3 |
| Duplicate events | 1 |
| Late events detected | 2 |
| Processing batches | 3 |
| Total revenue | 2,576,346.00 |
| Average order value | 2,571.20 |
| Customer totals | 5 |

The benchmark was processed in batches of 500, 500, and 6 records.

A restart test confirmed that rerunning the pipeline against the completed benchmark database resumed from offset 1,006 without creating additional event outcomes or changing the aggregate totals.

## 9. Reconciliation

The reconciliation utility independently checks processing consistency.

Run benchmark reconciliation:

```bash
python main.py --input data/input/benchmark_events.jsonl --batch-size 500 --database data/output/benchmark.db --report reports/benchmark_summary.json
python reports/reconciliation.py --input data/input/benchmark_events.jsonl --database data/output/benchmark.db --report reports/benchmark_summary.json --output reports/benchmark_reconciliation.json
```

The verified benchmark reconciliation passed all six checks:

- Source record count and checkpoint consistency.
- Checkpoint does not exceed source records.
- Every checkpointed source record has a recorded outcome.
- Accepted, rejected, and duplicate outcomes reconcile.
- No pending source records remain.
- Aggregate report values match persisted processing results.

**Verified result: 6/6 checks passed, with zero pending records.**

## 10. Automated Tests

Run the complete test suite:

```bash
python -m unittest discover -v
```

Check Python syntax:

```bash
python -m compileall -q .
```

The current test suite contains 21 passing tests covering:

- Valid and invalid event processing.
- Duplicate detection.
- Late-event handling.
- Malformed JSON.
- Timestamp and timezone validation.
- Invalid customer identifiers.
- Invalid numeric values, including NaN and infinity.
- Database transaction rollback.
- Pipeline run creation and successful completion.
- Pipeline failure tracking.
- Event-level run metadata.

**Latest verified test result: 21 tests passed.**

## 11. Reliability and Error Handling

StreamPulse includes several mechanisms to improve processing reliability:

- Checkpoint-based restart support.
- Atomic database transactions for batch persistence.
- Rollback when a batch write fails.
- Validation of malformed and invalid input records.
- Persistent pipeline execution status.
- Failure reason and end-time recording.
- Independent reconciliation of processing outcomes.

A report-generation failure was also tested. The pipeline correctly recorded the run as `FAILED` with the associated failure reason.
### Retry, Restart, Backfill, and Operational Assumptions

- **Rerun and idempotency:** When a pipeline is rerun against the same database and source file, processing resumes from the last committed source offset. Previously committed records are not inserted again.
- **Failure recovery:** Batch outcomes and checkpoint updates are committed transactionally. If a batch transaction fails, its changes are rolled back. The pipeline run is marked `FAILED` when an exception is handled. A subsequent manual rerun resumes from the last committed checkpoint.
- **Empty input:** An empty input file completes successfully with zero processing batches.
- **Schema changes:** Missing required fields are rejected with a validation reason. Additional fields are ignored by the current processor.
- **Timestamp assumptions:** Timestamps without an explicit timezone are interpreted as UTC. Timestamps with timezone information are normalized to UTC.
- **Backfill limitation:** The current checkpoint and source-offset design supports one source file per database. Processing a different source file with the same database can cause source-offset uniqueness conflicts. Use a separate database for an independent backfill.
- **Abrupt termination:** If the process is forcibly terminated before it can update its run status, the run may remain marked `RUNNING`. Manual inspection is required; automatic stale-run recovery is not currently implemented.
- **Automatic retries:** The pipeline does not currently implement automatic retry scheduling. Recovery is performed by manually rerunning the pipeline.

## 12. Design Considerations

- SQLite provides lightweight persistent storage without requiring a separate database server.
- JSONL supports sequential record ingestion.
- Micro-batching limits the amount of data processed in one batch.
- Checkpointing supports recovery from previously processed source offsets.
- Pipeline run metadata improves operational visibility.
- Reconciliation provides an independent consistency check.

## 13. Known Limitations

- The implementation is designed primarily for local, single-source processing rather than distributed execution.
- Event identifier and checkpoint constraints are not yet fully isolated across multiple independent input sources.
- Processing-state aggregation is not yet fully scoped by pipeline run.
- SQLite may not be appropriate for high-concurrency production workloads.
- The current pipeline does not implement distributed scheduling or parallel workers.

## 14. Future Improvements

- Add multi-source checkpoint isolation.
- Introduce configurable retry and dead-letter handling.
- Add structured logging and richer monitoring.
- Support larger-scale storage systems.
- Add distributed processing and scheduling.
- Introduce automated performance benchmarks and monitoring dashboards.

## 15. Conclusion

StreamPulse demonstrates an end-to-end micro-batch event processing workflow with validation, deduplication, late-event detection, transactional persistence, checkpoint recovery, aggregate reporting, pipeline run tracking, and independent reconciliation.

The implementation has been validated using a benchmark dataset, restart testing, controlled failure testing, and an automated test suite.

The project provides a practical demonstration of foundational data engineering and pipeline reliability concepts.


## 16. Performance Measurement

A dataset containing 100,000 generated events was used to compare two batch sizes. The generator produced 100,006 source records, including additional edge-case and malformed records.

Both runs used separate SQLite databases on the same Windows machine.

### Benchmark results

| Metric | Batch size 5 | Batch size 1,000 |
|---|---:|---:|
| Source records | 100,006 | 100,006 |
| Accepted | 100,002 | 100,002 |
| Rejected | 3 | 3 |
| Duplicates | 1 | 1 |
| Execution time | 74.47 seconds | 3.59 seconds |
| Reconciliation | PASS (6/6) | PASS (6/6) |

Batch size 1,000 was approximately **20.75 times faster**, reducing execution time by approximately **95.18%** in this measurement.

### Optimization

Larger batches reduce the number of database transactions required to process the same number of records. This reduces transaction overhead and improves throughput.

These timings are measurements from one machine and one benchmark run. Actual performance may vary depending on hardware, storage, and workload.

### Reproduce the benchmark

Generate the dataset:

```bash
python -m ingestion.event_generator --count 100000 --output data/input/large_events.jsonl
```

Run with batch size 5:

```bash
python main.py --input data/input/large_events.jsonl --batch-size 5 --database data/output/performance_5.db --report reports/performance_5.json
```

Run with batch size 1,000:

```bash
python main.py --input data/input/large_events.jsonl --batch-size 1000 --database data/output/performance_1000.db --report reports/performance_1000.json
```

Reconcile both runs using `reports/reconciliation.py` to verify that their outcomes and aggregate totals match.
