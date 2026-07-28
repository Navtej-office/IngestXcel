# Fabric Platform Constraints

Facts about Microsoft Fabric itself, confirmed directly (by testing or by Microsoft's own
documentation) — not decisions this project made, just things the platform does or doesn't
allow. Kept separate from `LESSONS_LEARNED.md` since these are true regardless of what we build.

---

### GUIDs must be lowercase in OneLake paths
`WORKSPACE_ID`/`LAKEHOUSE_ID`/`SQLDATABASE_ID`/`CONNECTION_ID` must be stored lowercase —
OneLake's path validation is case-sensitive and rejects uppercase, even though a GUID is
technically case-insensitive by definition.

### Lakehouse schema names: letters, numbers, and underscores only
Confirmed directly from Fabric's own error message — no hyphens allowed in a schema name.

### The Lakehouse SQL analytics endpoint is read-only
No DDL or data writes (CREATE SCHEMA, CREATE TABLE, INSERT/UPDATE/DELETE) can be executed
against it. Schema/table creation and all data modifications must go through the Lakehouse
Explorer UI or a Spark notebook.

### Activities nested inside a Switch case or If Condition cannot be referenced from outside it
Bridge required values across scope via a pipeline variable, set immediately after the nested
activity — safe only because the `ForEach` containing it is Sequential, not Parallel.

### Fabric/ADF `ForEach` cannot be nested inside another `ForEach`
No native way to build an "outer loop over systems, inner loop over that system's tables"
structure directly. Governs how cross-system vs. within-system parallelism has to be achieved
(see ADR-0001) — via independent trigger fires for the outer dimension, a flat `ForEach` for the
inner one, not literal nesting.

### `ForEach`'s `batchCount` cannot be a dynamic expression — static value only
Must be a literal number set on the activity, never a pipeline parameter or expression. Hard
platform ceiling is 50. Microsoft's own guidance recommends starting conservative (5–10) for
real workloads, since concurrency is bounded by connections to actual source systems, not just
Fabric compute capacity.

### Spark's Hive-inherited catalog is case-insensitive by default
Any schema or table created via notebook (`CREATE SCHEMA`/`CREATE TABLE`) is silently normalized
to lowercase unless `spark.conf.set("spark.sql.caseSensitive", True)` is explicitly set
beforehand in that session. True across all Spark environments, not Fabric-specific. Fabric's
Copy-activity-driven "Auto create table" path appears to preserve case correctly (unlike raw
notebook `CREATE TABLE`) — the risk is specifically with notebook/Spark-SQL-based creation.

### Fabric's native Schedule Trigger: Minute/Hour/Day/Week, no native Monthly (yet)
Minute-level granularity is supported (e.g. every 15 minutes) — finer than hourly, not coarser.
No native Monthly trigger as of this writing ("in plan" per Microsoft's own community
responses); the documented workaround is a Daily trigger plus a cheap conditional check inside
the pipeline ("is today the target day?") that exits immediately otherwise. Hard limit: max 20
schedule triggers per pipeline — a 21st distinct cadence needs a new pipeline.

### Invoke Pipeline has two versions with different connection requirements
**Legacy**: same-workspace only, no Connection object needed at all. **Preview/new**:
cross-workspace capable, lets you monitor the child run separately in Monitoring Hub, but
**always** requires a Connection (a "Fabric Data Pipelines" type, OAuth2-based) regardless of
whether the cross-workspace capability is actually needed. For same-workspace orchestration,
Legacy is simpler and avoids the connection setup entirely.

### High Concurrency mode for notebooks in pipelines requires matching default Lakehouses
Lets multiple pipeline-invoked notebook steps share one Spark session instead of each starting
its own — but only if the notebooks have the same default Lakehouse, the same Spark compute
configuration, the same library packages, and are run by the same user. Two notebooks that
legitimately need different default Lakehouses (e.g. a Bronze notebook and a Silver notebook in
a medallion architecture) can never satisfy this, regardless of how the feature is configured.

### A pipeline-invoked notebook's Spark session stops automatically; an interactive one doesn't
Calling `mssparkutils.notebook.exit()` from a notebook run *by a pipeline* stops the Spark
session as part of that call. Calling it *interactively* (manual/standalone testing) throws an
exception, skips remaining cells, but **keeps the session alive** — it has to be stopped
explicitly (`mssparkutils.session.stop()`, or the "Stop Session" button in the notebook toolbar).

### Trial-tier Fabric capacity allows exactly one concurrent Spark session
No bursting, no job queueing — a second session request errors out (`TooManyRequestsForCapacity`,
HTTP 430) rather than waiting. Not something a notebook-level or pipeline-level setting can route
around (see `LESSONS_LEARNED.md`); resolves on Premium/paid capacity tiers.

###The Lakehouse SQL analytics endpoint does not support column-mapped Delta tables
delta.columnMapping.mode='name' lets a Delta table store column names Delta would otherwise reject (spaces, and other special characters) — but a table created with column mapping enabled fails to load at all through Fabric's Lakehouse SQL analytics endpoint, which breaks Power BI, Direct Lake, and any ad-hoc SQL query against it, even though direct Spark reads of the same table continue to work fine. A known, still-open Fabric limitation, not something resolved by any client-side setting. The practical implication: normalize column names to remove invalid characters at ingestion time instead of enabling column mapping to work around them.

###A Delta table registered via a direct path write can end up with stale SQL-catalog metadata
Writing to a target path with df.write.format("delta").mode(...).save(path) — with no prior CREATE TABLE registration — creates the Delta table correctly and the write itself succeeds. But querying that table afterward via its three-part SQL catalog name (SELECT * FROM lakehouse.schema.table) can throw IllegalStateException: Couldn't find <column>#<id> in [...] — the catalog's cached schema has gone out of sync with the table's actual Delta log. Confirmed by comparing DESCRIBE TABLE output (the catalog's view) against a direct spark.read.format("delta").load(path).printSchema() (the real Delta log), which showed a genuinely different column list. This is a known class of Spark/Delta bug tied specifically to catalog registration via direct path writes rather than an explicit CREATE TABLE, and it reproduces in Fabric.

###Spark infers NullType ("void") for a column that is entirely null across a batch
When spark.createDataFrame() is called without an explicit schema, a column with no non-null value anywhere in the batch to sample from gets inferred as NullType. Delta and Parquet cannot physically persist a NullType column at all — it is silently dropped from the written file, even though the write reports success and the SQL catalog still reflects an attempt to write it (this is one concrete way the catalog-vs-Delta-log mismatch above can arise). Avoiding this requires passing an explicit schema (e.g. a StructType with every column typed StringType) to spark.createDataFrame() rather than relying on inference from the data, regardless of how thoroughly the data was pre-processed beforehand.
