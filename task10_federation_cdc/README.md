# Lab 10 — External operational databases: Lakehouse Federation & CDC

External source: Azure SQL Database `adventureworks` on `sql-lena066636-lab10.database.windows.net`
(serverless free offer). Schema `sales` holds a TPC-H subset: 1000 customers, 2000 products and their
orders, with order dates moved to 2024–2030. Databricks side: workspace trial_3, catalog
`dbr_dev_ua_5816_trail_3`, schema `lab10`.

```
Azure SQL (sales.customers, products, orders)
   │  SQL Server CDC: cdc.sales_customers_CT, cdc.lsn_time_mapping
   ▼
connection lena066636_azure_sql ──▶ foreign catalog lena066636_adventureworks
   │                                   │
   │ query in place, join with Delta   │ CDC events after the last LSN
   ▼                                   ▼
01_federation                     03_cdc_merge ──▶ customer_changes (all events)
                                                ├▶ customers_current (MERGE: insert / update / delete)
                                                └▶ customers_history (SCD type 2)
```

## Job `lab10-federation-cdc`

| Task | Notebook | What it does |
|---|---|---|
| `setup_source` | `00_setup_source` | creates the source tables, enables CDC on `sales.customers`, loads data, clears the Delta targets |
| `cdc_initial_load` | `03_cdc_merge` | first run: the 1000 insert events from CDC |
| `federation` | `01_federation` | connection and foreign catalog, queries, pushdown plan, federation vs Delta, join with Delta |
| `simulate_changes` | `02_simulate_changes` | inserts, updates, deletes in Azure SQL, waits for CDC capture |
| `cdc_incremental` | `03_cdc_merge` | second run: only the new events, validated against the source |

The Azure SQL password lives in the secret scope `lab10`. The connection references it with
`secret('lab10', 'azure-sql-password')`, and the notebooks that write to the source read it with
`dbutils.secrets.get`, so it never appears in code or output.

## Part A — Lakehouse Federation

- `CREATE CONNECTION ... TYPE sqlserver` and `CREATE FOREIGN CATALOG ... OPTIONS (database 'adventureworks')`
  expose the database as `lena066636_adventureworks.<schema>.<table>` in Unity Catalog.
- External tables are queried with plain SQL. `EXPLAIN FORMATTED` shows the part of the query sent to
  SQL Server: filters, projections and simple aggregates are pushed down.
- The same aggregation runs over the federated table and over a Delta snapshot (`orders_snapshot`).
  The results are equal; the Delta query is faster, because federation pays for the round trip to
  Azure SQL and for the source's own compute.
- Federated `sales.orders` is joined with the Delta table `dim_date` from the Lab 8 gold layer.

### Federation vs ingestion

| | Federation | Ingestion into Delta |
|---|---|---|
| Freshness | live, every query sees the current source | as fresh as the last load |
| Latency | network round trip and source compute on every query | local Delta, data skipping, caching |
| Load on the source | every analytical query hits the operational database | one extraction per load |
| Scale | limited by the source and by what can be pushed down | Spark and Delta scale independently |
| Cost | no copy, no pipeline | storage and a pipeline to run |
| Good for | exploration, small lookups, data that changes constantly, one-off joins | dashboards, heavy or repeated queries, history |

### Security

- Credentials are stored once, in the connection, encrypted by Databricks; users never see them. The
  password comes from a secret scope.
- Access is governed by Unity Catalog: `USE CONNECTION`, `USE CATALOG`, `SELECT` on the foreign catalog,
  with lineage and audit like any other catalog.
- Every query runs in the source as the connection user, so that user should have read-only rights on
  the needed schemas only (here it is the server admin, acceptable for a lab only).
- The source must be reachable from Databricks: the Azure SQL firewall allows Azure services; in
  production a private endpoint or network connectivity configuration is the better choice.

## Part B — Change Data Capture

SQL Server CDC records every change of `sales.customers` in `cdc.sales_customers_CT` with its LSN.
`03_cdc_merge` reads the events with `__$start_lsn` greater than the stored watermark through the
foreign catalog, adds the commit time from `cdc.lsn_time_mapping` and:

1. appends them to `customer_changes`, the audit trail of the batch;
2. applies the last event per key to `customers_current` with `MERGE INTO`
   (`D` → delete, matched → update, not matched → insert);
3. maintains `customers_history` as SCD type 2: the first event of a key closes its open version,
   every insert or update becomes a version valid until the next event of the key, a delete only closes;
4. moves the watermark after all targets are written.

`02_simulate_changes` covers the tricky cases: two updates of one row in a batch, an update followed by
a delete, an insert followed by an update, an insert followed by a delete. After each run the mirror
is compared with the live source through federation and the job fails on any difference.
`customers_current` has Delta Change Data Feed on, so downstream consumers can read its changes the
same way.

### Batch vs CDC

| | Batch (full or by `updated_at`) | CDC |
|---|---|---|
| Deletes | invisible unless the source keeps soft deletes | captured as events |
| Intermediate states | lost, only the state at load time | every change, in commit order |
| Load | rescans the table or an index | reads only the change log |
| Source requirements | none | CDC enabled, log retention, capture job |
| Complexity | low | ordering, watermarks, idempotent apply |

### Slowly changing dimensions and late-arriving data

- The history table is SCD type 2, the same model as Lab 4, but driven by real change events with
  their commit time instead of comparing snapshots.
- Events are ordered by LSN and sequence value, not by arrival, so several changes of a row in one
  batch, or events that arrive late, are applied in commit order.
- The watermark is an LSN, so a rerun after a failure re-reads the same events; the history `MERGE`
  matches on LSN and sequence value and does not insert a version twice.
- An event older than the open version of its key (for example after a manual reprocessing) would need
  the open version split instead of closed; with LSN ordering from one source this does not happen.

## Run

```bash
cd task10_federation_cdc
databricks bundle deploy -p trial3
databricks bundle run -p trial3 lab10_federation_cdc
```
