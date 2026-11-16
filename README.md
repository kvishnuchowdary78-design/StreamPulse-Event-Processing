# StreamPulse — Micro-Batch Event Processing and Monitoring System

## 1. Project Overview

StreamPulse is a Python-based event-processing pipeline that simulates e-commerce order events using JSONL input and configurable micro-batches.

It demonstrates ingestion, validation, deduplication, late-event detection, transactional persistence, checkpoint recovery, and aggregate reporting.

The system uses Python and SQLite and requires no external services or paid cloud infrastructure.

## 2. Key Features

- JSONL event ingestion with source-offset tracking
- Configurable micro-batch processing
- Checkpoint-based restart and recovery
- Event schema and business-rule validation
- Duplicate event detection
- Out-of-order and late-event detection
- Invalid event quarantine
- Transactional SQLite persistence
- Batch-level logging and processing metrics
- Customer-level revenue aggregation
- Configurable database and report paths
- Automated reconciliation of source, database, and report
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

### Ingestion Layer
- Reads JSONL records.
- Assigns source offsets.
- Divides records into configurable batches.
- Resumes from the last saved checkpoint.

### Processing Layer
- Parses and validates incoming events.
- Checks required fields and data types.
- Rejects malformed or invalid records.
- Detects duplicate event IDs.
- Identifies late and out-of-order events.

### Storage and Reporting Layer
- Stores accepted, rejected, and duplicate outcomes in SQLite.
- Saves batch outcomes and checkpoints transactionally.
- Generates overall and customer-level aggregates.
- Produces JSON reports.
- Reconciles source records against database outcomes.

## 5. Project Structure

```text
StreamPulse/
├── config.py
├── main.py
├── requirements.txt
├── README.md
├── DESIGN_NOTES.md
├── DATA_DICTIONARY.md
├── ingestion/
│   ├── event_generator.py
│   └── event_ingestion.py
├── processing/
│   └── event_processor.py
├── storage/
│   └── database.py
├── data/
│   ├── input/
│   │   ├── events.jsonl
│   │   └── benchmark_events.jsonl
│   └── output/
├── reports/
│   ├── aggregate_report.py
│   ├── aggregate_summary.json
│   ├── reconciliation.py
│   ├── reconciliation_summary.json
│   ├── benchmark_summary.json
│   └── benchmark_reconciliation.json
├── logs/
└── tests/
    ├── test_pipeline.py
    └── test_storage.py
