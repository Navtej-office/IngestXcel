# ADR-0008: Bronze file source support (Excel first)

## Status
Accepted

## Context
The framework so far only ingests from SQL sources (Fabric SQL Database, Azure SQL
Database), both via the existing `EXECUTION_METHOD` split (PIPELINE / NOTEBOOK).
We need to support file-based sources — starting with Excel, with CSV/JSON/XML
planned as follow-on additions using the same mechanism.

Checked the ISD Accelerator reference framework first. It documents file
ingestion (CSV/JSON/Excel/XML/Parquet) as reading directly from a Lakehouse
`Files` area — either uploaded directly, or via a OneLake shortcut to another
Fabric location, ADLS Gen2, S3, or GCS — with per-format config (e.g. Excel
needs a `sheet_name` value) and automatic incremental loading based on file
modification time, no watermark column required. It does **not** prescribe
where or how contributors get files into that Files area in the first place —
that's an operational decision left to the implementer, not a framework
mechanism. The choice below is therefore an original decision, not one
borrowed from the reference.

## Decision
1. **New source tech type: `LAKEHOUSE_FILE`**, not `EXCEL_FILE`. The
   *connection* (which Lakehouse's Files area holds the data) is identical
   across every file format; only the per-entity reader logic differs by
   `FILE_FORMAT`. This avoids re-registering a connection endpoint when
   CSV/JSON/XML are added next.
2. **New Switch case `NOTEBOOK_LAKEHOUSE_FILE`** in `PL_Bronze_Ingestion`,
   routing to a new notebook, `NB_Bronze_File_Ingestion`. Unlike the existing
   NOTEBOOK cases, this needs **no preceding staging Copy activity** — the
   file already lives in Lakehouse Files, so there's nothing external to
   stage. `FILE_FORMAT` is branched on inside the notebook; only `EXCEL` is
   implemented for now, any other value raises a clear "not yet supported"
   error rather than failing silently.
3. **Upload mechanism, for now: direct upload into `Bronze_LH`'s own Files
   area**, not a OneLake shortcut to ADLS. Decided on the population expected
   to contribute files — people who already have, or can reasonably get,
   Fabric workspace access. Revisit an ADLS-backed shortcut (e.g. a shared
   `TempWorkArea` container) if/when contributors outside that population need
   to drop files without holding a Fabric role.
4. **Confirmed mechanism (important for future changes): switching from
   direct upload to a OneLake shortcut later requires zero code change.**
   `notebookutils.fs` and `pandas.read_excel` (via local temp copy) see a
   shortcut-backed path exactly the same as a natively-stored file — OneLake
   presents a unified namespace regardless of backing storage. The only
   change needed is the new entity's `WILDCARD_FOLDER_PATH` metadata value
   pointing at wherever the shortcut lands, plus the one-time shortcut
   creation itself (a real action, not automatic).
5. **Incremental logic is file-modification-time based, not a metadata
   column.** No `WATERMARK_COLUMN` is set for file entities; `PROCESSING_METHOD`
   still governs FULL vs INCREMENTAL, but the comparison happens against each
   file's `modifyTime` before it's even opened, not a SQL predicate.
6. **FULL entity, zero files matched this run → no-op, not a truncate.**
   Deliberately did not overwrite Bronze with an empty result when no files
   match — "nothing in the folder right now" is treated as more likely a
   transient or misconfigured path than a genuinely empty source. Silently
   emptying a FULL table on every path hiccup was judged the wrong default.
7. **No key required, same as the existing keyless-entity design** — an Excel
   entity with no reliable key is handled exactly like `PersonDetails` was
   (blank `PRIMARY_KEYS`, NOTEBOOK-only, Silver falls back to the no-watermark
   full-rescan SCD2 path if history is needed).
8. **Silver requires zero changes.** `NB_Silver_SCD_Load` reads from the
   Bronze Delta table only — it has no awareness of whether the data
   originated from SQL, staged Parquet, or an Excel file. This confirms the
   medallion boundary is doing its job.

## Consequences
- New `META_CONNECTION_PROPERTY`/config vocabulary needed: `FILE_FORMAT`,
  `WILDCARD_FOLDER_PATH`, `SHEET_NAME`, `FILE_HAS_HEADER_ROW` (Excel-specific
  for now; CSV/JSON/XML will each need their own subset when added).
  `WATERMARK_COLUMN` stays unused for this tech type.
- `NB_Bronze_File_Ingestion` is new code, separate from
  `NB_Bronze_Staged_Ingestion` — they don't share a Switch case or a staging
  step, only the same Bronze write-mode split (overwrite for FULL, append for
  INCREMENTAL, no key needed for either).
- Multi-sheet Excel entities (a comma list or `*`) get unioned with a
  `sheet_name` column appended; a single named sheet does not get that
  column. Multiple matched files also get a `_source_file_name` column for
  lineage (our own addition, not something the reference documents).
- Access control for uploaded files currently rides on Fabric workspace
  permissions (whoever can write to `Bronze_LH`'s Files area). If the ADLS
  shortcut path is adopted later, access control moves to Azure RBAC/ACLs on
  the storage container instead — a meaningfully different permission model,
  worth a deliberate decision when that day comes, not a silent side effect.
