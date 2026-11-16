
# StreamPulse — Data Dictionary

## 1. Input Event Schema

Input file: `data/input/events.jsonl`

Format: JSON Lines (one JSON event per line).

| Field | Data Type | Description | Validation |
|---|---|---|---|
| event_id | String | Unique event identifier | Required, non-empty |
| order_id | String | Order identifier | Required, non-empty |
| customer_id | String | Customer reference | Must match seeded customer data |
| event_type | String | Type of event | Must be ORDER_PLACED |
| amount | Number | Order amount | Must be positive |
| event_time | ISO 8601 String | Time when the event occurred | Must be a valid timestamp |
| arrival_time | ISO 8601 String | Time when the event reached the pipeline | Must be a valid timestamp |

## 2. Ingestion Metadata

These fields are added by the ingestion and processing pipeline.

| Field | Data Type | Description |
|---|---|---|
| offset | Integer | Sequential input line number |
| raw | String | Original input record |
| source_offset | Integer | Source line number associated with a processed event |
| is_late | Boolean | Indicates whether the event arrived behind the maximum event time observed |

## 3. Customer Reference Data

The sample generator uses five seeded customer references:

- C001
- C002
- C003
- C004
- C005

Events containing other customer references are rejected.

## 4. Database Schema

### Table: processed_events

Stores valid, accepted events.

| Column | Data Type | Description |
|---|---|---|
| event_id | TEXT | Primary key |
| order_id | TEXT | Order identifier |
| customer_id | TEXT | Customer reference |
| event_type | TEXT | Event type |
| amount | REAL | Order amount |
| event_time | TEXT | Event occurrence timestamp |
| arrival_time | TEXT | Event arrival timestamp |
| is_late | INTEGER | 1 if late, otherwise 0 |
| source_offset | INTEGER | Unique source offset |

### Table: rejected_events

Stores invalid records.

| Column | Data Type | Description |
|---|---|---|
| id | INTEGER | Auto-increment primary key |
| source_offset | INTEGER | Unique source offset |
| raw_data | TEXT | Original input record |
| reason | TEXT | Validation or parsing failure reason |

### Table: duplicate_events

Stores duplicate records.

| Column | Data Type | Description |
|---|---|---|
| id | INTEGER | Auto-increment primary key |
| source_offset | INTEGER | Unique source offset |
| event_id | TEXT | Duplicate event identifier |
| reason | TEXT | Duplicate detection reason |

### Table: checkpoints

Stores processing progress.

| Column | Data Type | Description |
|---|---|---|
| source_file | TEXT | Primary key identifying the input file |
| last_processed_offset | INTEGER | Last committed source offset |

## 5. Processing Metrics

| Metric | Meaning |
|---|---|
| total_orders | Number of accepted events |
| total_revenue | Sum of accepted event amounts |
| average_order_value | Average amount across accepted events |
| late_orders | Number of accepted events marked late |
| rejected_events | Number of invalid input records |
| duplicate_events | Number of duplicate records |
| batches_processed | Number of batches processed in a pipeline run |
| last_processed_offset | Last successfully committed source offset |

## 6. Customer Aggregate Fields

The customer summary in `reports/aggregate_summary.json` contains:

| Field | Description |
|---|---|
| customer_id | Customer reference |
| order_count | Number of accepted orders |
| total_amount | Total accepted order amount |
| average_amount | Average accepted order amount |
| late_orders | Number of accepted late orders |

## 7. Rejection Reasons

Possible rejection reasons include:

- Malformed JSON
- Expected a JSON object
- Missing required field
- Invalid event_id
- Invalid order_id
- Invalid customer reference
- Invalid event_type
- Amount must be a positive number
- Invalid timestamp

## 8. Sample Dataset Reconciliation

| Category | Count |
|---|---:|
| Input records | 26 |
| Accepted records | 22 |
| Rejected records | 3 |
| Duplicate records | 1 |
| Total classified records | 26 |
| Accepted late events | 2 |

Late events are a subset of accepted events and are not added again to the input reconciliation total.

## 9. Aggregate Report Location

Output file:

`reports/aggregate_summary.json`

The report contains overall processing aggregates, data-quality counts and customer-level summaries.
