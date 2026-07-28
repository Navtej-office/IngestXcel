# ADR-0007: Silver Layer v1 Scope & Design

**Status:** Accepted, built, and proven (SCD1, SCD2, and quarantine paths all tested with
deliberate before/after tests, not just single ambiguous runs)

## Context

The ISD Accelerator reference framework's full Curating-Data feature surface (merge types,
transformation library, DQ rule engine, post-write reconciliation, surrogate keys, custom
functions) is enterprise-scale and far beyond what a first Silver build needs. Industry-standard
medallion architecture practice was used to decide what actually earns the name "Silver" versus
what's framework richness accumulated over time — same philosophy as the Gold placeholder
(ADR-0006) and skipping ISD's full Bronze engine (ADR-0005): build only what's needed now,
document the rest as a known deferral rather than silently omitting it.

## Decision: v1 Feature Scope

**Must-have** (this is what makes it Silver, not optional):
- Merge/upsert logic: both SCD1 (simple upsert — the default for most entities) and SCD2
  (historized with start/end date + current flag)
- Deduplication: resolve to one row per primary key within a batch before merging, keeping the
  latest by watermark value
- Primary-key uniqueness enforcement: a duplicate primary key after dedup is quarantined, not
  silently merged and not a whole-batch failure
- Schema enforcement: target Delta table has a defined, typed schema
- Basic cleansing only: blank-to-null, string trimming — not the full transformation library
- Correct incremental extraction from Bronze via the same watermark-column mechanism already
  proven in Bronze (not Change Data Feed for v1)
- Quarantine mechanism: rows failing validation are written to an actual quarantine table
  (inspectable later), not merely counted; `ROWS_REJECTED` reflects the count
- Auditability: reuses the existing `META_INGESTION_LOG`/`spStart`/`spComplete_Ingestion_Log`
  pattern as-is

**Should-have:** post-write row-count reconciliation (warn-level, not blocking); schema drift
policy of auto-evolve via `mergeSchema`, same posture as Bronze; soft-delete propagation
capability documented but not implemented until a real source has a delete-indicator column.

**Not now** (explicitly deferred, not silently omitted): surrogate key generation and
dimension-table attachment (industry-standard placement is Gold — Kimball dimensional modeling —
not Silver); the full transformation library; the rich DQ rule engine beyond basic PK/null
checks; PII masking; custom-function escape hatches; execute-only delegated integrations.

Two defaults chosen without a further confirmation round: (1) incremental extraction stays
watermark-column-based, not CDF, since Bronze tables don't have CDF enabled and enabling it
retroactively is a separate task; (2) quarantined rows physically land in a table, not just a
count.

## Decision: Metadata Design

Silver source entities are named plain (e.g. `person_address`), never layer-prefixed (never
`bronze.person_address`) — confirmed against ISD Accelerator's own convention that
`Target_Datastore` is always a separate field from the entity name, never concatenated into it.
The Bronze table a Silver entity reads from is resolved via `SOURCE_CONNECTION_ENDPOINT_ID`
pointing at Bronze's own target Lakehouse endpoint (`TGT_INGESTXCEL_BRONZE_LH`), reused as a
source reference — no new Fabric Connection needed.

Per ISD Accelerator's confirmed convention (each layer transition gets its own orchestration
row — a Bronze load and a Silver load for the same business entity are two separate
`META_ORCHESTRATION_ID` rows, never one shared row), each Silver entity has its own
`META_SOURCE_ENTITY` row and its own `META_ORCHESTRATION` row.

`SOURCE_SCHEMA_NAME` (Bronze's schema alias, needed to build the full
`Tables/{schema}/{table}` read path) requires an extra join back through Bronze's own
orchestration row, since `BRONZE_SCHEMA_ALIAS` lives on Bronze's *original source* endpoint, not
on the shared Bronze Lakehouse target endpoint Silver otherwise reuses. That join must be
constrained to Bronze's specific Lakehouse endpoint, not just an entity-name match — Bronze and
Silver commonly share the same table name by design, so a name-only join would also match
Silver's own row.

Per-system Silver triggers are required (e.g. `TRG_FABRICTRAINING_INGESTXCEL_SILVER_LOAD_DAILY`),
never one flat shared name across systems — matching ADR-0001's per-system orchestration model.

`SCD_TYPE` (values `'SCD1'`/`'SCD2'`), `WATERMARK_COLUMN`, `SCD2_START_DATE_COL`,
`SCD2_END_DATE_COL`, `SCD2_CURRENT_FLAG_COL`, and `QUARANTINE_TABLE_NAME` are all flat
`META_CONFIGURATION_CORE` rows, category `SILVER`. Deliberately **no** separate `DEDUP_KEY`
field (`PRIMARY_KEYS` is reused — no known entity needs the two to differ) and **no**
`META_CONFIGURATION_ADVANCED` usage (SCD2 settings are always exactly one fixed set per entity;
`ADVANCED`'s instance-numbered EAV pattern is reserved for config that genuinely repeats).

## Decision: Pipeline & Notebook Design

`PL_Silver_Ingestion` mirrors `PL_Bronze_Ingestion`'s shape but is structurally simpler: SCD1 vs
SCD2 branching happens entirely inside `NB_Silver_SCD_Load` via `SCD_TYPE`, so no Switch
Execution Route is needed at the pipeline level — every Silver entity takes the same single path
through one notebook activity. `ForEach` set to `isSequential=false`, `batchCount=10`.

**SCD2 algorithm**, adapted directly from ISD Accelerator's proven pattern: hash-compare business
columns (excluding PK, watermark, and SCD2 bookkeeping columns) between the new batch and
existing active (`IS_CURRENT='Y'`) rows. No existing match = brand new; hash differs = changed;
hash matches = unchanged, no action at all (this is what prevents spurious new SCD2 versions on
every run). Changed rows produce two derived rows — the old version stamped
`IS_CURRENT='N'`/`SCD_END_DATE=`new timestamp, and the new version stamped `IS_CURRENT='Y'`/
`SCD_END_DATE=NULL` — unioned with brand-new rows, then a single Delta merge whose match
condition (`PK AND current.IS_CURRENT='Y' AND new.IS_CURRENT='N'`) only matches the close-out
rows; everything else falls through to `whenNotMatchedInsertAll`. `SCD2_START_DATE_COL`
deliberately reuses the same natural business timestamp as the watermark column rather than a
separate system-generated timestamp — matches ISD Accelerator's own `source_timestamp_column_
name` convention. SCD1 is a plain `whenMatchedUpdateAll`/`whenNotMatchedInsertAll` upsert — no
history, values fully overwritten in place.

## Consequences

Both SCD1 (Person.BusinessEntity) and SCD2 (Person.Address) proven via deliberate before/after
tests — baseline checked, source changed, result re-checked — not just a single ambiguous run.
An early SCD2 test result was genuinely confusing (`rows_merged=0` looked like a bug) until
checking Silver's **full** table (not just `IS_CURRENT='Y'` rows) revealed the merge had already
succeeded correctly on an earlier attempt — the lesson being to always check full history, not
just the active-row view, when validating SCD2 behavior. Quarantine path also proven, via a
synthetic null-primary-key row correctly landing in a physical quarantine table.

Not yet done: extending Silver to the remaining Bronze entities beyond these first two, and the
full E2E regression test across all of them together.
