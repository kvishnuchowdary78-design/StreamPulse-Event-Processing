
# StreamPulse — Real-Time Event Processing and Monitoring System

## 1. Project Overview

StreamPulse is a Python-based event-driven data processing system that simulates real-time e-commerce order events using micro-batches.

It demonstrates how incoming events can be ingested, validated, deduplicated, processed, stored and monitored.

The system uses Python and SQLite without requiring external services or paid cloud infrastructure.

## 2. Key Features

- JSONL event ingestion
- Configurable micro-batch processing
- Source offset tracking and checkpoint recovery
- Event schema and business-rule validation
- Duplicate event detection
- Out-of-order and late-event detection
- Invalid event quarantine
- Transactional SQLite storage
- Batch-level logging and processing metrics
- Customer-level revenue aggregation
- Automated JSON aggregate reporting
- Unit tests for processing and storage

## 3. Technology Stack

| Component | Technology |
|---|---|
| Programming language | Python 3.10+ |
| Database | SQLite |
| Input format | JSON Lines (JSONL) |
| Output format | JSON |
| Logging | Python logging |
| Testing | unittest |
| Version control | Git |

No third-party Python packages are required.

## 4. Architecture

The project follows three main layers:

### Ingestion Layer
- Reads JSONL event records.
- Assigns source offsets.
- Divides records into batches.
- Supports checkpoint-based resumption.

### Processing Layer
- Parses incoming JSON.
- Validates required fields and data types.
- Rejects invalid events.
- Detects duplicate event IDs.
- Identifies events arriving out of event-time order.

### Storage and Reporting Layer
- Stores accepted, rejected and duplicate events in SQLite.
- Saves checkpoints transactionally with batch data.
- Calculates overall and customer-level aggregates.
- Generates a JSON report.

## 5. Project Structure

```text
StreamPulse/
│
├── config.py
├── main.py
├── requirements.txt
├── README.md
├── DESIGN_NOTES.md
├── DATA_DICTIONARY.md
│
├── ingestion/
│   ├── __init__.py
│   ├── event_generator.py
│   └── event_ingestion.py
│
├── processing/
│   ├── __init__.py
│   └── event_processor.py
│
├── storage/
│   ├── __init__.py
│   └── database.py
│
├── data/
│   ├── input/
│   └── output/
│
├── checkpoints/
├── logs/
├── reports/
│
└── tests/
    ├── __init__.py
    ├── test_pipeline.py
    ├── test_storage.py
    ├── test_database.py
    ├── test_database_summary.py
    └── reset_for_replay.py
```

## 6. Setup and Execution

### Requirements

- Python 3.10 or later
- Windows, Linux or macOS

### Generate sample events

```powershell
python -m ingestion.event_generator
```

### Run the processing pipeline

```powershell
python main.py
```

### Generate the aggregate report independently

```powershell
python -m reports.aggregate_report
```

### Run unit tests

```powershell
python -m unittest discover -v
```

### Audit the database

```powershell
python -m tests.test_database_summary
```

## 7. Sample Dataset

The included sample generator creates:

- 20 normal order events
- 1 duplicate event
- 1 event with a missing customer reference
- 1 event with an invalid amount
- 2 out-of-order or late events
- 1 malformed JSON record

Total input records: 26.

## 8. Verified Processing Results

| Metric | Result |
|---|---:|
| Input records | 26 |
| Accepted events | 22 |
| Rejected events | 3 |
| Duplicate events | 1 |
| Late events | 2 |
| Batches processed | 6 |
| Final source offset | 26 |

The two late events are included in the accepted-event count.

## 9. Aggregate Results

| Metric | Result |
|---|---:|
| Total orders | 22 |
| Total revenue | 50823 |
| Average order value | 2310.14 |
| Late orders | 2 |

Customer-level aggregates are saved in:

`reports/aggregate_summary.json`

## 10. Database Tables

- `processed_events`: accepted events
- `rejected_events`: invalid or malformed events
- `duplicate_events`: duplicate event records
- `checkpoints`: last successfully processed source offset

## 11. Monitoring

Pipeline execution logs are written to:

`logs/streampulse.log`

The logs include pipeline start and completion, source offsets, batch metrics, report generation and errors.

## 12. Testing

The project includes six automated tests covering:

- Valid event processing
- Duplicate detection
- Invalid amount rejection
- Late-event detection
- Malformed JSON rejection
- Database transaction rollback

Latest verification: 6 tests passed.

## 13. Design Limitations

- Event ingestion uses a local JSONL file rather than a distributed message broker.
- SQLite is used instead of a distributed database.
- Late-event detection is based on event-time ordering relative to the maximum event time observed; it does not implement a configurable watermark.
- The sample customer reference data is seeded and limited to five customers.
- The project simulates near-real-time processing through repeated micro-batches; it is not a continuously running streaming service.

## 14. Future Improvements

- Integrate Kafka or another message broker.
- Implement configurable event-time watermarks.
- Add configurable batch sizes.
- Add monitoring dashboards and alerting.
- Add cloud deployment and distributed processing.
- Expand automated tests for restart and failure scenarios.

## 15. Summary

StreamPulse demonstrates a complete local event-processing workflow, including ingestion, validation, deduplication, late-event handling, transactional persistence, checkpoint recovery, monitoring and aggregate reporting.
