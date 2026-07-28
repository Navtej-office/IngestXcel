# ADR-0004: Bronze Layer Operational Decisions

**Status:** Accepted (growth management not yet implemented — see Consequences)

## Context

Three related Bronze-layer operational questions: how does an unbounded append-only table stay
manageable long-term; how does the framework choose between a plain Copy-activity pipeline and a
notebook per entity; and how are failed runs retried without an operational metadata flag that
risks CI/CD drift.

## Decision

**Bronze growth management**: Bronze remains append-only by design (never truncated). Growth is
controlled via (1) partitioning by `INGESTION_BATCH_DT`, (2) a retention/purge policy — default
180 days, to be confirmed — enforced by a scheduled maintenance job, not the ingestion pipeline
itself, and (3) governance review at onboarding to ensure `PROCESSING_METHOD = FULL` is only
assigned to small, low-volume reference tables.

**Execution method**: `META_CONFIGURATION_CORE`, `CONFIGURATION_CATEGORY = 'BRONZE'`,
`CONFIGURATION_NAME = 'EXECUTION_METHOD'`, value `'PIPELINE'` or `'NOTEBOOK'`. Required and
explicit per Bronze entity at onboarding — never defaulted, never silently inferred. `PIPELINE`
is the expected default recommendation for new tables (cheaper, proven path); `NOTEBOOK` is a
deliberate opt-in exception (see ADR-0005 for when and why). Scoped to Bronze only — Silver and
Gold are always `NOTEBOOK` by construction, so storing this value for them would be inert
metadata never read by anything.

**Restart-from-failure**: handled entirely via a runtime pipeline parameter, never a metadata
flag — toggling a persistent field for operational restarts creates CI/CD drift and risks
silently disabling a table if someone forgets to revert it.
- `TargetEntityIds` (String, default `''` = process everything) — a comma-separated list of
  `META_ORCHESTRATION_ID` values. When populated, only those orchestration rows are processed;
  used to retry just the entities that actually failed, without re-processing succeeded work
  (critical for `FULL`-load entities, which would otherwise get a redundant duplicate append on
  every retry).
- `spGet_Bronze_Batch`/`spGet_Silver_Batch` both accept this parameter and filter accordingly.
- A helper procedure, `spGet_Failed_Entities(@TriggerName)`, returns a ready-to-paste
  comma-separated list of the most recent run's failed `META_ORCHESTRATION_ID`s, so a retry
  never requires manually reading the log table.
- `TargetEntityIds` threads through `PL_Master_Orchestrator` as well as the individual layer
  pipelines, so a Bronze-only retry doesn't require re-invoking Silver.

## Consequences

Growth management is not yet implemented — tracked as a follow-up build phase. Execution method
and restart-from-failure are both fully built and proven across every entity tested so far.
