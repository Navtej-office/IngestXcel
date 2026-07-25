# CLAUDE.md — Metadata-Driven Ingestion Framework (Microsoft Fabric)
> Last updated: 2026-07-25

> This file is persistent project context for Claude — kept deliberately short. It covers
> what's timeless (project overview, non-negotiable principles, conventions) and points to
> dedicated files for everything else. Read this file plus whichever linked file is relevant to
> the task at hand — not the whole project history every time.

---

## Where to find things

| Need | File |
|---|---|
| Why we built X this way, what alternatives were considered | `docs/decisions/000N-*.md` (one ADR per decision — see index below) |
| A bug we already hit and how it was fixed | `LESSONS_LEARNED.md` |
| A fact about what Fabric does or doesn't allow | `PLATFORM_CONSTRAINTS.md` |
| The metadata database's table definitions | `DB_SCHEMA.md` |
| Current task status, estimates, what's done | The project plan spreadsheet |

**ADR index:**
- `0001` — Orchestration and scheduling model (`PL_Master_Orchestrator`, per-system triggers, `batchCount`)
- `0002` — Onboarding governance & table provisioning ("use existing, never auto-create")
- `0003` — Schema-per-source-system & connection design
- `0004` — Bronze layer operational decisions (growth management, execution method, restart-from-failure)
- `0005` — Bronze NOTEBOOK execution path (Stage-to-Files, `SOURCE_QUERY_OVERRIDE`, exit-value contract)
- `0006` — Gold placeholder scope
- `0007` — Silver layer v1 scope & design (SCD1/SCD2, metadata design, pipeline structure)

When a new significant decision gets made, add a new ADR rather than editing an old one in
place — if it supersedes an earlier decision, say so in the new ADR and mark the old one
superseded, don't rewrite history.

---

## 1. Project Overview

**What this is:** A metadata-driven data ingestion framework built in Microsoft Fabric. Source
and target systems are defined in a metadata table (the metadata database is a Fabric SQL DB in
the same workspace); the framework reads that metadata, dynamically resolves connections, and
copies data from source to target. A companion log table records execution results for
monitoring and troubleshooting. The target is always Fabric (Lakehouse or Warehouse). The source
can be anything — Azure SQL DB, Oracle, PostgreSQL, MySQL, DB2, ADLS, API, Excel, etc. Fabric
uses medallion architecture: Bronze, Silver, and Gold.

**Primary execution layer:** Hybrid — leveraging both Fabric Pipelines and Notebooks. Pipelines
handle ingestion from supported Azure/RDBMS/SaaS sources and orchestrate Bronze-layer loading
(cost-effective, always AS-IS/raw). Notebooks handle custom or dynamically-configured sources,
and all Silver/Gold transformation and loading logic. The execution engine is metadata-driven —
Pipeline vs. Notebook is selected per entity based on source type, layer, and load configuration.

**Fabric items in use:**
- Workspace `IngestXcel` — metadata store (Fabric SQL DB) plus target Lakehouses (`Bronze_LH`,
  `Silver_LH`).
- Workspace `FabricTraining` — acts as a source system for development/testing (its own SQL DB).
- Notebooks/Pipelines: no project-specific naming convention beyond what's in Section 5 below —
  otherwise follow Microsoft's own guidance.
- Environment/Spark pool config: use the appropriate runtime, node size, and autoscale setting.

---

## 2. Non-Negotiable Principles

These are timeless operating rules, not a decision log — they don't change as the project
evolves. Do not silently change or "improve" them; if a change seems warranted, flag it and ask.

1. **No credentials in the metadata table.** Only a `connection_ref` (Fabric Connection
   name/GUID) or a Key Vault secret name — never a connection string, username, password, or API
   key.
2. **Auth resolution is explicit per row**, via `META_CONNECTION_ENDPOINT.AUTH_TYPE`:
   `'ENTRA_MANAGED_IDENTITY'`, `'SERVICE_PRINCIPAL'`, `'KEY_VAULT_SECRET'`, `'GATEWAY_SQL_AUTH'`.
   Never assume one auth mechanism for all sources.
3. **Entra ID / workspace identity first.** For any source/target supporting Entra auth, use
   workspace identity / managed identity — no secret at all.
4. **Key Vault for anything needing a secret**, resolved at runtime via
   `notebookutils.credentials.getSecret(...)` (notebook layer) or a Key Vault connection
   reference (pipeline layer). Secrets are never persisted to variables that get logged, written
   to files, or printed in cell output.
5. **Gateway routing** (on-prem/VNet) is a property of the Fabric Connection object itself, not
   something the framework's code should encode or branch on directly.
6. **Idempotency**: every ingestion run must be safely re-runnable without duplicating or
   corrupting data. Bronze full loads append a new tagged batch, never truncate. Bronze
   incremental loads use a watermark-based append strategy. Silver/Gold incremental loads use an
   appropriate idempotent strategy (partition overwrite or merge/upsert on business key) per
   entity's metadata.
7. **Logging is mandatory** for every run, including failures — both pipeline- and
   activity-level.
8. **Inactive metadata**: metadata tables may have inactive records; always filter to active
   only when reading.
9. **Metadata naming convention**: metadata tables and columns are always upper case.
10. **Structured data** is ingested to Tables in the Lakehouse.
11. **File naming** (where files are used): `SourceName/yyyymmdd/sourcetablename_yyyymmdd_HH_MM_SS`.
12. **SCD**: SCD1 or SCD2, implemented in the Silver layer, configurable and driven from
    metadata (see ADR-0007).
13. Bronze supports dynamic metadata-driven connectivity, configurable Full/Incremental load,
    audit/operational metadata capture, append-only Delta storage, and extensibility to new
    source systems/formats through the same metadata-driven mechanism. Schema evolution is not
    automatic by default — entities requiring drift-safety use the NOTEBOOK path (ADR-0005).
14. Silver supports metadata-driven dedup on configurable business keys, SCD2 where applicable,
    configurable data quality/validation rules, schema enforcement, and metadata-driven
    transformation/standardization prior to load.
15. **Framework extensibility**: an external (to the framework) notebook or pipeline can be
    invoked, with orchestration and logging still handled through the framework
    (`EXECUTION_ARTIFACT_TYPE` on `META_ORCHESTRATION` — see `DB_SCHEMA.md`).
16. **FK enforcement**: `META_CONNECTION_PROPERTY.META_CONNECTION_ENDPOINT_ID` is DB-enforced via
    FK because the metadata store is Fabric SQL Database (enforced FKs supported, unlike Fabric
    Warehouse). If the metadata store ever moves to Warehouse, this FK must be dropped/redeclared
    NOT ENFORCED, and endpoint-ID validation would need to move into the write path.
17. **Log table has no FK** to avoid resolving it at execution time — a business key is kept
    instead.
18. **Schedule frequency**: native Fabric scheduling/triggers, minimum supported frequency once
    per hour, designed to accommodate lower frequencies too (see ADR-0001 for the actual
    implementation and `PLATFORM_CONSTRAINTS.md` for Fabric's real trigger granularity limits).

---

## 3. Non-Functional Requirements

- **Error handling:** one failed table in a trigger batch never stops the whole batch — continue
  and log the failure per row.
- **Concurrency:** see ADR-0001 for the actual per-system/per-layer parallelism design.
- **Retry policy:** max retries = 3, configured as a per-activity property (not metadata-driven)
  on each pipeline activity connecting to an external source. Applied to Copy/Lookup activities
  touching the source system; logging-only activities (`Start Log`, `Complete Log
  Success/Failure`) are lower priority for this.
- **Alerting:** Fabric alerts, Teams, email.

---

## 4. Conventions

- **Table/schema naming**: all Bronze (and Silver/Gold) target-side schema and table names are
  **lowercase** (e.g. `fabrictraining_ingestxcel`, `person_businessentitycontact`) — see
  `PLATFORM_CONSTRAINTS.md` for why (Spark's default lowercasing behavior). This applies to
  `BRONZE_SCHEMA_ALIAS`, `TARGET_ENTITY`, and their Silver/Gold equivalents — source-side names
  (the external system's own naming) are untouched. Metadata columns/values unrelated to target
  object naming (`PROCESSING_METHOD`, `EXECUTION_METHOD`, `AUTH_TYPE`, etc.) keep the standard
  uppercase metadata convention (Principle #9 above) — this lowercase rule is scoped specifically
  to physical target schema/table names.
- **Trigger naming**: `TRG_{SYSTEM_IDENTIFIER}_{LAYER}_LOAD_{FREQUENCY}` — one trigger per source
  system, per layer, per schedule frequency, never one shared trigger across systems (see
  ADR-0001). `{SYSTEM_IDENTIFIER}` must exactly match that source's `BRONZE_SCHEMA_ALIAS`.
- **Pipeline naming**: `PL_{Layer}_Ingestion` (e.g. `PL_Bronze_Ingestion`, `PL_Silver_Ingestion`)
  for the per-layer entity-processing pipelines; `PL_Master_Orchestrator` for the
  system-level wrapper.
- **Stored procedure naming**: `sp<Verb>_<Entity>` (e.g. `spGet_Bronze_Batch`) — never the `sp_`
  prefix, which forces SQL Server to check `master` first on every call and risks colliding with
  a system procedure name.
- **Notebooks/general code style/branching**: follow Microsoft's/industry best practices; no
  additional project-specific convention beyond what's stated above.

---

## 5. How to Work With Me on This Project

- Build in small, reviewable units — not "the whole framework" in one pass.
- Before generating code, restate which of Section 2's non-negotiable principles apply to the
  current task.
- Flag any assumption you're making explicitly rather than silently picking one — especially
  around auth type, load type, or error handling for a new source type not yet covered.
- If a request conflicts with a Section 2 principle or an existing ADR, say so before proceeding
  rather than implementing the conflict.
- Check the reference framework (ISD Accelerator) first for any genuine design ambiguity, and
  state explicitly whether it matches before proposing an original approach.
- New significant decisions become a new ADR in `docs/decisions/`, not an edit to `CLAUDE.md`
  itself. New bugs/gotchas go in `LESSONS_LEARNED.md`. New Fabric platform facts go in
  `PLATFORM_CONSTRAINTS.md`. Schema changes go directly into `DB_SCHEMA.md`.
