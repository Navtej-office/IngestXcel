# ADR-0005: Bronze NOTEBOOK Execution Path

**Status:** Accepted, built, and proven across two source tech types

## Context

Some Bronze entities have columns Fabric's Copy activity cannot write directly to a Delta table
sink — e.g. Person.Address's `SqlGeography` column. `LakehouseTableSink` has to map every column
to a Delta type; a geography column has no such mapping and the write fails outright.

## Decision

**Stage-to-Files then Notebook-Load.** For `EXECUTION_METHOD = 'NOTEBOOK'` Bronze entities, the
Switch case (e.g. `NOTEBOOK_FABRIC_SQLDB`) runs two activities, not one: a Copy activity
("Stage \<Source\> Source") extracts via a `sqlReaderQuery`/`oracleReaderQuery` into a
`ParquetSink` under Lakehouse Files — deliberately not a Delta table sink — then a notebook
(`NB_Bronze_Staged_Ingestion`) reads the staged Parquet and appends + `mergeSchema`-writes it to
the Bronze Delta table. Staging to files instead of a typed table sink is what avoids the
type-mapping failure: `ParquetSink` just serializes whatever the query returns, so any needed
type conversion (e.g. `.STAsText()` for a geography column) happens in the query text itself,
before the type-mapping problem can occur.

Pattern adapted from the ISD Accelerator reference framework's `pipeline_stage_and_batch`
approach, deliberately scoped down to Bronze-only/append-only — none of its Silver/Gold
SCD-merge, schema-drift-logging, or custom-function machinery was pulled in.

**`SOURCE_QUERY_OVERRIDE`** (new `CONFIGURATION_NAME`, category `BRONZE`, `NOTEBOOK`-only): when
present, replaces the pipeline's auto-generated `SELECT * FROM ... WHERE ...` with a
hand-authored query — where an onboarding admin writes any required explicit type conversion
directly in SQL, rather than the framework attempting automatic type introspection against
`INFORMATION_SCHEMA`. Stores the column list only, never a `WHERE` clause — the pipeline's Stage
Copy activity still owns watermark filtering entirely, appending it via `@concat()` onto
whichever base query it's using (override or auto-generated), keeping watermark mechanics in
exactly one place. `PIPELINE` entities are entirely unaffected.

**Notebook exit-value contract (standing, all layers):** every `NOTEBOOK`-method notebook, at
every medallion layer, ends by calling `mssparkutils.notebook.exit()` with a JSON object of this
exact shape: `{status, rows_read, rows_written, rows_rejected, watermark_value_used,
watermark_value_new}`. `rows_written` means total rows affected at the target. Declared before
Silver existed, specifically so a future Silver notebook could reuse the exact same
`Complete Log Success`/`Failure` Script-activity pattern without a per-layer branch. On a genuine
failure, the notebook lets the exception propagate uncaught (after step-tagging it — see below)
so the Notebook activity itself reports Failed.

**Step-tagged error handling:** adapted from ISD Accelerator's `Activity_Run_Logs`/`Step_Name`
pattern, scoped down deliberately — each logical phase of a notebook is wrapped in its own
try/except, re-raising with a fixed step prefix (e.g. `[Load Staged Parquet To Bronze] Delta
write failed: <short reason>`) rather than adding a second structured logging table. Chosen
because these notebooks are a handful of cells; revisit a real step-log table only if
complexity grows enough to need it.

**Retry policy:** the staging Copy activity keeps `Retry=3` (safe — each attempt re-reads the
source into the same idempotent per-run staging path). The notebook Load activity is
deliberately `Retry=0`: it performs a real Delta append, and an automatic retry after "the write
succeeded but something downstream failed" would silently double-append rows. Recovery from a
failed Notebook activity happens only via a full pipeline re-run or `TargetEntityIds`.

## Consequences

Proven across two source tech types: Person.Address (`NOTEBOOK_FABRIC_SQLDB`, the original
`SqlGeography` case) end-to-end — backfill, no-op idempotency, genuine incremental append,
and a deliberately-reproduced failure path — and `dbo.DimDate` (`NOTEBOOK_AZURE_SQL`, cut over
from `PIPELINE` to prove the pattern generalizes). Critically, `NB_Bronze_Staged_Ingestion`
itself needed **zero changes** between the two — it only ever reads whatever Parquet was staged
and writes Delta, with no awareness of which source connector produced the staged files. Real
validation that the design decouples the notebook from source-specific concerns.

**Known limitation, accepted for now:** staged Parquet files live under
`bronze_staging/{schema}/{entity}/{PipelineStartTime}/`, one folder per run. A run that fails
and is never restarted leaves its staged files behind indefinitely — no automatic cleanup
exists yet. Low priority while volumes are small; revisit once more `NOTEBOOK` entities exist.
