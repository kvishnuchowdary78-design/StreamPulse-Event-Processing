
# StreamPulse — Design Notes

## 1. Objective

StreamPulse demonstrates an event-driven data processing pipeline for simulated e-commerce order events.

The main objective is to process incoming records in small batches while maintaining data quality, duplicate protection, checkpoint recovery and monitoring.

## 2. Architecture

The system uses three logical layers.

### Layer 1: Ingestion

Module: `ingestion/event_ingestion.py`

Responsibilities:
- Read JSONL input records.
- Assign sequential source offsets.
- Divide input into configurable batches (default batch size: five).
- Resume reading from the last saved offset.

The event generator is available in `ingestion/event_generator.py`.

### Layer 2: Processing

Module: `processing/event_processor.py`

Responsibilities:
- Parse JSON records.
- Validate required fields.
- Check customer references and event types.
- Validate positive numeric amounts.
- Validate timestamps.
- Detect duplicate event IDs.
- Detect events arriving behind the maximum event time observed.

Invalid records are rejected with a reason. Duplicate records are tracked separately.

### Layer 3: Storage and Reporting

Modules:
- `storage/database.py`
- `reports/aggregate_report.py`

Responsibilities:
- Store accepted events.
- Store rejected events.
- Store duplicate events.
- Persist processing checkpoints.
- Calculate customer-level and overall aggregates.
- Generate a JSON report.

## 3. Processing Flow

1. Initialize the SQLite database.
2. Read the last processed source offset.
3. Restore previously processed event IDs and maximum event time.
4. Read the next configured batch of records.
5. Parse and validate each record.
6. Separate accepted, rejected and duplicate records.
7. Identify out-of-order events.
8. Save batch results and checkpoint in one database transaction.
9. Update processing metrics.
10. Repeat until all input records are processed.
11. Generate the aggregate report.

## 4. Checkpoint Strategy

The pipeline uses source offsets to track processing progress.

The checkpoint is stored in the SQLite `checkpoints` table.

Each batch saves:
- Accepted events
- Rejected events
- Duplicate events
- Last processed source offset

These writes occur within one SQLite transaction.

If a transaction fails, its writes are rolled back. This prevents a checkpoint from advancing independently of its corresponding batch data.

On restart, the pipeline resumes from the last committed offset.

## 5. Deduplication Strategy

The `event_id` field uniquely identifies an event.

Previously accepted event IDs are restored from the database when the pipeline starts.

If an incoming event ID already exists in the processed-event set, it is classified as a duplicate and recorded in `duplicate_events`.

The database also enforces a primary key on `event_id`.

## 6. Late-Event Strategy

The processor maintains the maximum event time observed.

If a valid event arrives with an event time earlier than that maximum, it is marked as late.

Late events are still accepted and stored.

This implementation uses maximum event-time ordering. It does not implement a configurable watermark or allowed-lateness window.

## 7. Error Handling

The pipeline handles:
- Malformed JSON
- Non-object JSON records
- Missing required fields
- Invalid customer references
- Unsupported event types
- Invalid timestamps
- Non-positive or invalid amounts
- Database transaction failures

Rejected records retain their original raw data and rejection reason.

Unexpected pipeline errors are logged with exception details and re-raised.

## 8. Database Design

### processed_events

Stores validated and accepted events.

Primary key: `event_id`

Unique constraint: `source_offset`

### rejected_events

Stores invalid input records and rejection reasons.

Unique constraint: `source_offset`

### duplicate_events

Stores duplicate event IDs and their source offsets.

Unique constraint: `source_offset`

### checkpoints

Stores the latest successfully committed source offset for each input file.

### pipeline_runs

Stores one execution record per pipeline run, including the UUID run ID, source file, UTC start and end timestamps, status, and failure reason. Runs begin as RUNNING and finish as SUCCESS or FAILED.

## 9. Monitoring

Python logging records:
- Pipeline start and completion
- Starting source offset
- Batch-level accepted count
- Rejected count
- Duplicate count
- Late-event count
- Report generation
- Unexpected errors

Log file: `logs/streampulse.log`

## 10. Aggregate Reporting

The reporting module calculates:
- Total accepted orders
- Total revenue
- Average order value
- Late-event count
- Rejected-event count
- Duplicate-event count
- Customer-level order counts and revenue

Output file:

`reports/aggregate_summary.json`

## 11. Transaction and Recovery Considerations

Batch records and their checkpoint are committed together.

A failed database transaction rolls back all writes in that batch.

The pipeline restores its processing state from SQLite when restarted.

The sample implementation uses a single local input file and a single processing process. It is not designed for concurrent distributed consumers.

## 12. Known Limitations

- Local file-based ingestion rather than Kafka.
- SQLite rather than distributed storage.
- No configurable watermark.
- No continuous background streaming service.
- Fixed sample customer reference data.
- No external monitoring dashboard.

## 13. Future Enhancements

- Kafka-based event ingestion.
- Configurable event-time watermarks.
- Retry and dead-letter queue strategies.
- Distributed storage.
- Real-time monitoring dashboard.
- Cloud deployment.
- Concurrent processing and scalability testing.
