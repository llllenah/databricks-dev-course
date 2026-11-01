# Lab 11 — Zero-bus streaming with Zerobus Ingest

A producer pushes order events directly into a Unity Catalog Delta table through Zerobus Ingest, with
no broker in between. Workspace trial_3 (eastus), catalog `dbr_dev_ua_5816_trail_3`, schema `lab11`.

```
producer (Zerobus SDK, gRPC, service principal)
   │  at-least-once, a replay arrives as duplicates
   ▼
events_bronze        every delivery, managed Delta table
   │  readStream + foreachBatch MERGE on event_id
   ▼
events_silver        one row per event
```

## Job `lab11-zerobus`

| Task | Notebook | What it does |
|---|---|---|
| `setup` | `00_setup` | creates the tables, grants the service principal, resets the previous run |
| `produce` | `01_producer` | pushes `seq` 0–9999, measures time to durable and to queryable |
| `silver` | `02_silver` | merges new bronze rows into silver |
| `produce_replay` | `01_producer` | restarts from `seq` 9000 and pushes 9000–10999: 1000 repeats, 1000 new events |
| `silver_after_replay` | `02_silver` | merges only the rows added since the last checkpoint |
| `validate` | `03_validate` | asserts the row counts, no duplicates in silver, replayed events are identical |

## Authentication

Zerobus accepts OAuth, so the producer runs as the service principal `lena066636-zerobus-lab11`. Its
secret is in the secret scope `lab11`. The principal has only what a writer needs: `USE CATALOG`,
`USE SCHEMA`, and `MODIFY, SELECT` on `events_bronze`.

## Idempotent processing

Zerobus is at-least-once with no built-in deduplication, so a retry can write a record twice. Handling
that takes three parts:

1. **Stable key.** `event_id` is built from `producer_id` and `seq`, and the payload is derived from the
   same values, so a retry produces the same record, not a new one.
2. **Raw bronze.** Bronze keeps every delivery, so duplicates stay visible and can be audited.
3. **Insert-only `MERGE`.** Silver inserts an `event_id` only if it is absent. Reprocessing a micro-batch
   after a failure therefore has no effect.

## Results

Run [709919067593170](https://adb-7405613488297663.3.azuredatabricks.net/?o=7405613488297663#job/642461038607865/run/709919067593170), all tasks green.

| Step | Records | Durable (flush) | Queryable in bronze | Silver MERGE |
|---|---:|---:|---:|---|
| initial | 10 000 | 0.27 s | 5.66 s | 10 000 source rows, 10 000 inserted |
| replay | 2 000 | 0.20 s | 3.86 s | 2 000 source rows, 1 000 inserted, 0 updated |

Final state: bronze 12 000 rows and 11 000 unique `event_id`, silver 11 000 rows and no duplicates. All
1000 replayed events match their originals.

An acknowledgement means durable, not queryable. The table caught up about 4–6 s later, in line with
the documented 5 s. Reads that must see just-written data have to wait for it, as `01_producer` does.
This is one small run from a notebook, not a benchmark.

## Kafka-style bus vs Zerobus

In Lab 3 the same kind of event went through Event Hubs: producer → hub → Spark consumer with its own
checkpoint → bronze. Here the producer writes to the table.

| | Bus (Kafka, Event Hubs) | Zerobus Ingest |
|---|---|---|
| Destinations | many, each consumer group reads on its own | one: a Unity Catalog Delta table |
| Components to run | brokers or a namespace, topics, partitions, consumer job | none, the service is serverless |
| Where data waits | in the topic, for the retention period | it is written to the table, unflushed buffers kept up to 28 days |
| Replay | any consumer rewinds its offset | re-read Delta history, or the producer sends again |
| Producer and table | decoupled, the schema can change without the producer knowing | coupled, records must match the table schema or they are rejected |
| Non-Databricks consumers | read the topic directly | must read from Delta |
| Delivery | at-least-once by default, exactly-once with idempotent producers and transactions | at-least-once, deduplication is downstream |
| Ordering | per partition | per stream |
| Security | broker credentials or SAS, ACLs per topic | Unity Catalog grants on the table |
| Cost | always-on brokers or throughput units, plus the consumer compute | serverless, billed by usage |

Use Zerobus when the lakehouse is the only destination and a bus would only act as a buffer. Keep a
bus when several systems consume the same events, when services react to each other, or when consumers
need to rewind independently.

## Discussion

**Event-driven pipelines.** Both designs react to events as they happen instead of polling a source.
Zerobus covers the ingest half: events reach Delta in seconds, and Structured Streaming or Lakeflow
pipelines take over from there. Fan-out still exists, but it happens after the table, through Delta
streaming reads, Change Data Feed or Delta Sharing, so it only suits consumers that can read Delta.

**Decoupled systems.** A bus decouples in time and in space: the producer does not know who reads, and
consumers can be down or added later. Zerobus keeps the decoupling in time, because a durable ack lets
the producer move on, and the table can be read by any number of later jobs. It gives up decoupling of
the destination and the schema: the producer names the table and must follow its schema, so changes
go table first, then producers.

**Cost and operations.** The bus costs include standing capacity, sizing partitions, upgrades, and a
consumer that runs all the time, even when no events arrive. Zerobus removes those and is billed by
usage against the Automated Serverless SKU. In exchange, there is one vendor and one destination, a
service principal per writer, and deduplication becomes the pipeline's responsibility. Real costs were
not measured here, because this workspace has no access to `system.billing`.

## Limits met in this lab

- The workspace must be in a supported region. eastus is supported, polandcentral is not.
- Target tables must be managed and must not be dropped or recreated while in use, so `00_setup`
  empties them with `DELETE` instead.
- Zerobus does not create tables or evolve schemas.

## Run

```bash
databricks bundle deploy -p trial3
databricks bundle run lab11_zerobus -p trial3
```

## Sources

- [Zerobus Ingest overview](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-overview)
- [Use Zerobus Ingest](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-ingest)
- [Zerobus limits](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-limits)
