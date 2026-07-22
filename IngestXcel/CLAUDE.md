# CLAUDE.md — Metadata-Driven Ingestion Framework (Microsoft Fabric)
> Last updated: 2026-07-20

> This document explains the Project overview, key architectural decision, data model etc for claude
> This file is persistent project context for Claude. Read it fully before making
> changes. Update it whenever an architecture decision, convention, or constraint
> changes — this file should always reflect the current state of truth, not the
> history of how we got here. 

---

## 1. Project Overview

**What this is:** A metadata-driven data ingestion framework built in Microsoft
Fabric. Source and target systems are defined in a metadata table, the metadata database is Fabric SQLDB in the same workspace; the framework reads that metadata, dynamically resolves connections, and copies data from source to target. A companion log table records execution results for monitoring and troubleshooting. The taget is always farbic ie. fabric lakehouse or fabric datawarehouse. The source can be anything like AzureSQLDB, Oracle, Postgresql, MYSQL, DB2 , ADLS,API , Excel etc.  Fabric will have medallion architecture Bronze, Silver and Gold.  


**Primary execution layer:** Hybrid: The metadata-driven ingestion framework shall adopt a hybrid  
    execution model to leverage the strengths of both Microsoft Fabric Pipelines and Notebooks.
    Pipelines shall be used for ingesting data from supported Azure,any RDBMS and SaaS source systems and for orchestrating data ingestion into the Bronze layer as it is more cost effective and in this layer it is always AS-IS loading.
    Notebooks shall be used for ingesting data from custom or dynamically configured source systems. Notebooks shall be used for implementing the transformation and loading logic for the Silver and Gold layers.
    The execution engine shall be metadata-driven, enabling the appropriate execution mechanism (Pipeline or Notebook) to be selected based on the source type, processing layer, and load configuration.
**Fabric items in use:**
- Workspace1: IngestXcel - workspace for Metdata framwork metadata store (FabricSQL) and Target Lakehouse 
  Bronze and Silver lakehouses. 
- Workspace2: FarbicTraining - Workspace act as the source. It has a SQLDB and we will take the table 
  from this database as source and populate the metadata table for development and testing. 
- Lakehouse: Bronze_LH - For Bronze Layer
- Lakehouse: Silver_LH - For Silver Layer
 
- Notebooks: No specific naming convention or folder structure  recommendation. Follow as per the
   microsoft guidelines 
- Pipelines: No specific naming convention recommendation. Follow as per the microsoft guidelines 
- Environment/Spark pool config: Use the appropriate Runtime, node size and autoscale setting  

---

## 2. Architecture Decisions (Non-Negotiable)

These decisions are already made. Do not silently change or "improve" them —
if a change seems warranted, flag it and ask before implementing.

1. **No credentials in the metadata table.** The metadata table stores only a
   `connection_ref` (a Fabric Connection name/GUID) or a Key Vault secret name
   — never a connection string, username, password, or API key.
2. **Auth resolution is explicit per row**, via META_CONNECTION_ENDPOINT.AUTH_TYPE column with
   values: 'ENTRA_MANAGED_IDENTITY','SERVICE_PRINCIPAL','KEY_VAULT_SECRET','GATEWAY_SQL_AUTH'. Connection-resolution logic branches on this field —
   never assume one auth mechanism for all sources.
3. **Entra ID / workspace identity first.** For any source/target that
   supports Entra auth (Azure SQL, ADLS Gen2, Synapse, Fabric SQL endpoints),
   use workspace identity / managed identity — no secret at all.
4. **Key Vault for anything needing a secret**, resolved at runtime via
   `notebookutils.credentials.getSecret(...)` (notebook layer) or a Key Vault
   connection reference (pipeline layer). Secrets are never persisted to
   variables that get logged, written to files, or printed in cell output.
5. **Gateway routing** (on-prem / VNet) is a property of the Fabric Connection
   object itself, not something the framework's code should encode or branch
   on directly.
6. **Idempotency:** The metadata-driven ingestion framework shall ensure that every ingestion run is  
    idempotent, allowing failed or interrupted executions to be safely re-run without creating duplicate or inconsistent data.
    Full Loads: Bronze layer full loads shall append the complete source extract as a new batch (tagged with an ingestion batch ID/timestamp) — Bronze is never truncated. Truncate-and-load applies only to Silver/Gold current-state tables where full history is not required.
    Incremental Loads (Bronze Layer): The framework shall use a watermark-based append strategy to ingest only new or changed records since the previous successful execution.
    Incremental Loads (Silver and Gold Layers): The framework shall support an appropriate idempotent loading strategy, such as partition overwrite based on the watermark range or merge (upsert) using the business key, as defined in the metadata for each entity.
    The selected loading strategy shall ensure data consistency and prevent duplicate records across repeated executions.
7. **Logging is mandatory** for every run — no ingestion path should complete
   without writing a row to the log table, including on failure. Both pipeline and activities level logging should be enabled. 
8.  **Inactive Metadata** Metadata tables may have inactive records as well. While        
    reading meatdata always ensure only active records are read
9. **Metadata Naming Convention**Metadata tables and columns will be always in Upper Case
10. **Structured Data** All Structured data source should be ingested to Tables in Lakehouse
11. **Files in Lakehouse** The naming convention for files should be     
     SourceName\yyyymmdd\sourcetablename_yyyymmdd_HR_MM_SS 
12. **SCD**  SCD2 or SCD1  will be implemented in Silver layer which is configuratble and taken from
     metadata tables.  
13. **Bronze Layer Load** : The metadata-driven ingestion framework shall support the following 
   capabilities for Bronze layer ingestion:
   Dynamic connectivity using parameterized pipelines driven by metadata.
   Configurable load types, including Full Load and Incremental Load.
   Schema evolution is not automatic by default; eentities requiring drift-safety use a notebook-based load path (see item #25), configured explicitly per entity.
   Capture and maintenance of audit and operational metadata for each ingestion execution.
   Ingestion of structured data into append-only Delta tables to preserve the raw history of the source data.
   Extensibility to support multiple source systems and data formats through a common metadata-driven framework.
14. **Silver Layer Load** The metadata-driven ingestion framework shall support the following 
   capabilities for Silver layer processing:
   Metadata-driven deduplication of records based on configurable business keys and rules.
   Support for Slowly Changing Dimension (SCD) Type 2 processing where applicable.
   Configurable data quality and validation rules to identify and handle invalid or incomplete data.
   Schema enforcement to ensure conformity with the defined target data model.
   Metadata-driven transformation and standardization of data prior to loading into the Silver layer. 
15. **Framework Extensibility** The framework should support  extensibility hook so that if anyone 
    wanted  to invoke any external(to the framework) notebook or pipeline, orchastration and logging  should be able to do through framwork. 
16. **FK Enforcement** META_CONNECTION_PROPERTY.META_CONNECTION_ENDPOINT_ID is DB-enforced via FK 
    because the metadata store is Fabric SQL Database (which supports enforced FKs, unlike Fabric Warehouse). If the metadata store ever moves to Warehouse, this FK must be dropped or redeclared NOT ENFORCED, and endpoint-ID validation would need to move into the write path instead.
17. **LogTable** Log table approach should be optimsed as any FK in Log table will have to be resolved
    during the  execution time. Hence the FK is removed and Business key is kept 
18. **Execution and Orchestration Requirements**  Each medallion layer (Bronze, Silver, Gold), for 
    each source system, and for each schedule frequency, has its own trigger name and its own set of META_ORCHESTRATION rows — see Section 5's TRG_{SYSTEM_IDENTIFIER}_{LAYER}_LOAD_{FREQUENCY} convention (e.g. TRG_FABRICTRAINING_INGESTXCEL_BRONZE_LOAD_DAILY). A single trigger therefore contains only one system's tables at a time — it does not span multiple source systems.
    - Cross-system parallelism is achieved by scheduling multiple independent Fabric triggers concurrently, each invoking PL_Master_Orchestrator with a different SystemIdentifier parameter — not by grouping systems within a shared trigger's execution.
    - Layer sequencing within one system is enforced at the pipeline level: PL_Master_Orchestrator derives that system's Bronze/Silver/Gold trigger names from its SystemIdentifier and Frequency parameters, and invokes each layer's pipeline in strict order via Execute Pipeline activities with waitOnCompletion = true — Silver never starts before Bronze completes for that system, and Gold never starts before Silver completes.
    - Parallelism within one trigger (i.e., across the tables belonging to one system, at one layer) is achieved via a single flat ForEach with bounded concurrency (isSequential = false, Batch count = a MaxConcurrency pipeline parameter, default 4, configurable per run). This requires each Switch case / branch to be fully self-contained (its own logging completion steps referencing only its own activity outputs directly) — no pipeline variables shared across iterations, which are unsafe under concurrent execution.
    This requires no additional metadata schema beyond what Section 3 already defines — TRIGGER_NAME, scoped per system/layer/frequency per Section 5, is sufficient.


19. **Schedule Frequency Support** The metadata-driven ingestion framework shall support native fabric 
     scheduling /trigger feature and support  minimum scheduling frequency of once per hour. The framework shall also be designed to accommodate lower-frequency schedules (e.g., daily, weekly, or monthly)  
20. **Onboarding Governance Model-Layer Schema/Table Provisioning**
    Target schema and table(s) for all three layers (Bronze, Silver, Gold) are provisioned as a one-time manual setup step during consumer onboarding/change registration — never auto-created by any pipeline or notebook at runtime. This applies uniformly across layers; Silver and Gold structures (SCD tracking columns, business-logic outputs) cannot be mechanically inferred from source schema in the first place, making explicit DDL a hard requirement there, not merely a preference carried over from Bronze.
    - The framework is owned and governed by a central team. Consumers request onboarding via the governance forum and submit their metadata registration using the framework's specified template.
    - As part of registration, the consumer (or the central team on their behalf) creates the schema and target table(s) for every layer being registered, matching exactly what's declared in metadata (TARGET_ENTITY, BRONZE_SCHEMA_ALIAS, and their Silver/Gold equivalents once those layers are built).
    - All Copy/write activities use "Use existing", never auto-create. A run failing on a missing schema/table signals an incomplete registration, not a pipeline defect — and keeps the pipeline's own execution identity scoped to write/insert permissions only, with no CREATE TABLE/CREATE SCHEMA rights needed anywhere.
    - Schema drift is not silently auto-applied at any layer. Tested directly: Fabric's Copy activity, writing to an existing Delta table via implicit name-based mapping, neither errors nor adds a genuinely new source column — it's silently dropped. This confirmed the need for an explicit per-entity choice rather than relying on default Copy activity behavior.
    - Bronze schema-drift handling  is decided per entity via the EXECUTION_METHOD field — see item #25 for the current, generalized mechanism. (This item previously specified a narrower SCHEMA_EVOLUTION_REQUIRED flag scoped only to drift-safety; that has been superseded — schema drift is now just one of several reasons an entity might be registered with EXECUTION_METHOD = 'NOTEBOOK'.)
    - Silver/Gold schema drift: by design, changes do not need to automatically propagate beyond Bronze. A new source column landing in Bronze is sufficient; whether and how it flows into Silver/Gold is a deliberate, separate change managed through the same registration/approval process — not automatic.
21. **Schema-per-source-system** the Bronze schema a table lands under is resolved from the source 
    endpoint, not the target Lakehouse — stored as BRONZE_SCHEMA_ALIAS on the source's META_CONNECTION_ENDPOINT (e.g. FABRICTRAINING_INGESTXCEL). This prevents two distinct collision scenarios when a single Bronze Lakehouse hosts multiple source systems: two sources sharing a schema/table name, and one source having two schemas with an identically-named table (the latter already handled separately by keeping target table names schema-prefixed, e.g. PERSON_ADDRESS vs SALES_ADDRESS).  
22. **Connection Design** One Fabric Connection per physical source database, not one universal 
    connection per tech type.     
    Fabric's "SQL database" connector binds a Connection object to one specific database at creation time — it does not expose separate dynamic Workspace/Database override fields the way the Lakehouse connector does. The Connection's own GUID is stored as a CONNECTION_ID property on the source endpoint (renamed from the earlier FABRIC_CONNECTION_ID — this property isn't conceptually Fabric-specific once non-Fabric-native sources like Azure SQL Database also need one), and the Copy activity's Connection field is set to "Use dynamic content" referencing that stored ID. Adding a new physical source database requires creating one new Connection and recording its ID in metadata — no pipeline changes.
23. **Bronze growth management:** Bronze remains append-only by design (never truncated). Growth is controlled via (1) 
    partitioning     by INGESTION_BATCH_DT, (2) a retention/purge policy — default [confirm: 180 days] — enforced by a scheduled maintenance job, not the ingestion pipeline itself, and (3) governance review at onboarding to ensure PROCESSING_METHOD = FULL is only assigned to small, low-volume reference tables. Not yet implemented — tracked as a follow-up build phase alongside schema-drift handling.
24. **Fabric Constraints**
        (a) The lowercase-GUID rule. WORKSPACE_ID/LAKEHOUSE_ID/SQLDATABASE_ID/CONNECTION_ID must be stored lowercase — OneLake's path validation is case-sensitive and rejects uppercase, even though a GUID is technically case-insensitive.
        (b) The Lakehouse schema-naming rule. Confirmed directly from Fabric's own error message: schema names must contain only letters, numbers, and underscores — no hyphens.
        (c) The Lakehouse SQL analytics endpoint is read-only. No DDL or data writes (CREATE SCHEMA, CREATE TABLE, INSERT/UPDATE/DELETE) can be executed against it. Schema/table creation and all data modifications must go through the Lakehouse Explorer UI or a Spark notebook.
        (d) Activities nested inside a Switch case or If Condition branch cannot be referenced from outside that construct. Bridge required values across scope via pipeline variables, set immediately after the nested activity — safe only because the ForEach is Sequential, not Parallel.
        (e) There is no distinct "Azure SQL Database" connector in Fabric. Azure-hosted single databases are configured through the generic SQL Server connector type, differentiated only by providing an Azure server address — not a separately-labeled option in the connector picker. Azure SQL Managed Instance is a different, incompatible connector for a different Azure deployment model; selecting it instead of SQL Server for a single Azure SQL Database causes confusing, hard-to-diagnose runtime credential errors rather than a clear "wrong connector" message.
        (f) A copy-pasted activity can retain a completely different, incompatible connector type underneath its new name. Renaming a pasted activity (e.g. Get New Watermark → Get New Watermark__AzureSQLDB) does not change its underlying source type or connection settings — if the original was configured for a different connector (e.g. FabricSqlDatabaseSource), it silently stays that way. This only surfaces at runtime, as a confusing credential/auth error (e.g. "AccessToken not found" when the real issue is "wrong connector entirely"), not as an obvious configuration mismatch. After any copy-paste, verify the connector type and connection settings directly, not just the activity's name and visible parameters — when in doubt, delete and rebuild the activity fresh against the proven-working connector rather than trying to patch a pasted one.
        (g) Azure Key Vault reference fields must be set via the picker dialog, never typed as plain text. The Password field's AKV-reference icon opens a two-part picker (reference dropdown + separate secret-name box) — typing text directly into the Password field (even text that looks like a valid reference, e.g. refname>secretname) produces a garbled value that fails silently until "Test connection" or a real pipeline run is attempted, with an error ("password"/credential key not found) that doesn't point at the actual cause.
26. **Execution Method (Bronze)**META_CONFIGURATION_CORE, CONFIGURATION_CATEGORY = 'BRONZE',        
    CONFIGURATION_NAME = 'EXECUTION_METHOD', value 'PIPELINE' or 'NOTEBOOK'. Required and explicit per Bronze entity at onboarding — never defaulted, never silently inferred. Replaces the earlier, narrower SCHEMA_EVOLUTION_REQUIRED flag; schema-drift safety is now just one possible reason a consumer chooses NOTEBOOK, not the only one. PIPELINE is the expected default recommendation for new tables (cheaper, proven path); NOTEBOOK is a deliberate opt-in exception. Scoped to Bronze only — Silver and Gold are always NOTEBOOK by construction, so storing this value for them would be inert metadata never read by anything.
27. **Restart-from-Failure** Handled entirely via a runtime pipeline parameter, never a metadata 
    flag — toggling a persistent field for operational restarts creates CI/CD drift and risks silently disabling a table if someone forgets to revert it.
    - TargetEntityIds (String, default '' = process everything) — a comma-separated list of META_ORCHESTRATION_ID values. When populated, only those orchestration rows are processed; used to retry just the entities that actually failed, without re-processing succeeded work (critical for FULL-load entities, which would otherwise get a redundant duplicate append on every retry).
    - spGet_Bronze_Batch/spGet_Silver_Batch both accept this parameter and filter accordingly.
    - A helper procedure, spGet_Failed_Entities(@TriggerName), returns a ready-to-paste comma-separated list of the most recent run's failed META_ORCHESTRATION_IDs, so a retry never requires manually reading the log table.
    - TargetEntityIds threads through PL_Master_Orchestrator as well as the individual layer pipelines, so a Bronze-only retry doesn't require re-invoking Silver.
28. **Gold — Placeholder Scope for This Phase** Gold is not built this phase. The metadata model 
    reserves just enough shape to make a future Gold build additive rather than a redesign:
    - 'GOLD' is a valid value wherever trigger/layer naming conventions are validated.
    - META_CONFIGURATION_CORE.CONFIGURATION_CATEGORY = 'GOLD' is a reserved category name (mirroring 'BRONZE'/'SILVER'), with no values seeded yet.
    - No Gold stored procedures, no real PL_Gold_Load logic — only an empty placeholder pipeline shell exists (see Task 5.1).
    - Standing principle, decided now to avoid re-deriving later: a Gold entity's source is Silver's own target for that entity — the same "layer N reads from layer N-1's output, not the original external system" pattern already established for Silver reading from Bronze.
29. **Platform Constraint: Spark/Notebook Table Creation Lowercases Names by Default**
    Spark's catalog is case-insensitive by default (inherited Hive metastore behavior, true across all Spark environments, not Fabric-specific) — any schema or table created via notebook (CREATE SCHEMA/CREATE TABLE) is silently normalized to lowercase unless spark.conf.set("spark.sql.caseSensitive", True) is explicitly set beforehand in that session.
    Rather than requiring this flag to be remembered every time a new source is onboarded (a fragile, easy-to-miss manual step repeated indefinitely), the framework adopted lowercase as the standard target-naming convention instead (see Section 5) — this works with Spark's natural default rather than fighting it on every future table creation.
    Note: Fabric's Copy-activity-driven "Auto create table" path appears to preserve case correctly (unlike raw notebook CREATE TABLE), which is why earlier uppercase-named objects created that way worked without issue — the risk is specifically with notebook/Spark-SQL-based creation, which matters once schema-drift-safe (EXECUTION_METHOD = 'NOTEBOOK') entities are built.
30. **Bronze NOTEBOOK Execution Path — Stage-to-Files then Notebook-Load** 
    For EXECUTION_METHOD = 'NOTEBOOK' Bronze entities, the Switch case (NOTEBOOK_FABRIC_SQLDB) runs two activities, not one: a Copy activity ("Stage Fabric SQLDB Source") extracts via a SqlReaderQuery into a ParquetSink under Lakehouse Files — deliberately not a Delta table sink — then a notebook (NB_Bronze_Staged_Ingestion) reads the staged Parquet and appends + mergeSchema-writes it to the Bronze Delta table. Staging to files instead of a typed table sink is what avoids the failure that blocks the PIPELINE path on columns like SqlGeography: LakehouseTableSink has to map every column to a Delta type; ParquetSink just serializes whatever the query returns, so any needed type conversion happens in the query text itself (see item #31) before the type-mapping problem can occur. Pattern adapted from the ISD Accelerator reference framework's pipeline_stage_and_batch approach, deliberately scoped down to Bronze-only/append-only — none of its Silver/Gold SCD-merge, schema-drift-logging, or custom-function machinery was pulled in. Person.Address (SqlGeography column, NOTEBOOK_FABRIC_SQLDB) was the first NOTEBOOK entity, proven end-to-end: backfill, no-op idempotency, genuine incremental append (verified via direct Delta table query, not just the log), and a deliberately-reproduced failure path. dbo.DimDate (NOTEBOOK_AZURE_SQL) followed as the second, cutover from its original PIPELINE_AZURE_SQL registration — see item #39 for what that proved and item #40 for a real schema constraint discovered during the cutover.
31. **SOURCE_QUERY_OVERRIDE metadata field (NOTEBOOK-only)** New CONFIGURATION_NAME in 
    META_CONFIGURATION_CORE, category BRONZE, read only for EXECUTION_METHOD = 'NOTEBOOK' entities. When present, it replaces the pipeline's auto-generated SELECT * FROM ... WHERE ... with a hand-authored query — this is where an onboarding admin writes any required explicit type conversion (e.g. .STAsText() for a geography/geometry column) directly in SQL, rather than the framework attempting automatic type introspection against INFORMATION_SCHEMA. PIPELINE entities are entirely unaffected; they keep today's auto-generated query unchanged.
Decided: SOURCE_QUERY_OVERRIDE stores the column list only — it never includes a WHERE clause. The pipeline's Stage <Source> Copy activity still owns watermark filtering entirely, appending WHERE {WATERMARK_COLUMN} > '{last watermark}' via @concat() onto whichever base query it's using (override or auto-generated), exactly like it already does for PIPELINE entities. This keeps watermark mechanics in exactly one place — the pipeline — rather than duplicating that logic into every override string. Confirmed working against Person.Address's registered row.
32. **Notebook exit-value contract (standing, all layers — decided** now for Silver's benefit) 
    Every NOTEBOOK-method notebook, at every medallion layer (Bronze today; Silver/Gold later), must end by calling mssparkutils.notebook.exit() with a JSON object of this exact shape: {status, rows_read, rows_written, rows_rejected, watermark_value_used, watermark_value_new}.rows_written means total rows affected at the target — for Bronze that's rows appended;for a future Silver merge it would be rows inserted + updated. This is declared now, before  Silver exists, specifically so a future Silver notebook can reuse the exact same Complete Log Success/Complete Log Failure Script-activity pattern without a per-layer branch. On a genuine failure, the notebook must let the exception propagate uncaught (after step-tagging it — see item #33) rather than swallowing it, so the Notebook activity itself reports Failed and downstream Script activities can read @activity(...).error.message.
33. **Notebook error handling: step-tagged messages, not raw stack traces**
    Adapted from the ISD Accelerator's Activity_Run_Logs / Step_Name troubleshooting pattern, scoped down deliberately: rather than adding a second structured logging table, each logical phase of a Bronze/Silver/Gold notebook (e.g. read staged data, compute watermark, write target) is wrapped in its own try/except. On failure, the phase re-raises with a fixed step prefix — e.g. [Load Staged Parquet To Bronze] Delta write failed: <short reason> — so the single ERROR_MESSAGE line that lands in META_INGESTION_LOG says which step failed instead of a raw Spark stack trace. This is the minimum viable version of ISD Accelerator's multi-row, Step_Name-tagged log table — chosen over building a whole second table because today's Bronze notebooks are a handful of cells and one tagged line is enough signal. Revisit (a real step-log table) only if/when Silver's notebook complexity grows enough to need it.
34. **FABRIC_MONITOR_URL** now populated on every Complete Log Success/Failure META_INGESTION_LOG 
    has had this column since Phase 1, but no existing Complete Log Success/Failure Script activity — PIPELINE or NOTEBOOK — was actually populating it. Fixed as part of this build, for all cases: built from Fabric's pipeline system variables (@pipeline().DataFactory, @pipeline().Pipeline, @pipeline().RunId) so every logged run is one click away from the Fabric Monitor view, independent of whether the step-tagged ERROR_MESSAGE (item #33) is descriptive enough on its own.
35. **Retry policy**: Copy activity retries, Notebook activity does not The staging Copy activity
    keeps Retry=3, consistent with other Copy activities touching external sources — safe to retry because each attempt re-reads the source into the same idempotent per-run staging path (item #30). The notebook Load activity is deliberately Retry=0: it performs a real Delta append, and an automatic activity-level retry after a failure whose true cause was "the write succeeded but something downstream of it failed" would silently double-append rows. Recovery from a failed Notebook activity happens only via a full pipeline re-run or the existing TargetEntityIds restart mechanism (item #27), both of which re-derive the watermark fresh from META_INGESTION_LOG's last SUCCESS row rather than resuming an in-flight append.
36. **Known limitation, accepted for now: orphaned staged Parquet files ** - Staged Parquet files
     live under bronze_staging/{schema}/{entity}/{PipelineStartTime}/, one folder per pipeline run (idempotency idea borrowed from ISD Accelerator's trigger_time-named staging folders — a Copy-activity-level retry within the same run reuses the same path rather than proliferating files). A run that fails and is never restarted, or is restarted on a later run with a new PipelineStartTime, leaves its staged files behind indefinitely — no automatic cleanup exists yet. Accepted as low-priority tech debt while Person.Address is the only NOTEBOOK entity and volumes are small. Revisit once more NOTEBOOK entities are onboarded (options: scheduled cleanup of folders older than N days, or deleting the staging folder immediately after a successful load).
37. **Silver/Gold placeholder — CONFIGURATION_CATEGORY = 'SILVER' reserved (mirrors item #28)**
    Mirroring item #28's Gold placeholder: 'SILVER' is a reserved META_CONFIGURATION_CORE / META_CONFIGURATION_ADVANCED CONFIGURATION_CATEGORY value, with no values seeded yet beyond Person.Address's already-seeded SCD2_SETTINGS in META_CONFIGURATION_ADVANCED. Nothing else about today's Bronze NOTEBOOK build constrains Silver's design: SOURCE_QUERY_OVERRIDE (item #31) is Bronze/external-source-scoped only and irrelevant to Silver's Delta-to-Delta reads; NB_Bronze_Staged_Ingestion is not a notebook Silver extends — Silver gets its own notebook doing SCD merge logic; the one piece Silver must conform to is the exit-value contract in item #32.
38. **Lesson learned:**
- SQL Server permits duplicate column names in a plain SELECT list - Adding SOURCE_QUERY_OVERRIDE to spGet_Bronze_Batch initially produced a SELECT list with EXECUTION_METHOD projected twice — an additive diff got pasted alongside the original line instead of replacing it. T-SQL compiles and runs this without error, silently returning two same-named columns, which is undefined behavior for a Fabric pipeline expression like item().EXECUTION_METHOD. Fixed by replacing the whole affected SELECT block rather than appending to it. Worth remembering for any future proc column addition: check the SELECT list for duplicate output names by eye, since SQL Server won't warn you.
-    Fabric/ADF pipeline activity dependencies are always a logical AND across multiple dependsOn entries, never an OR — there's no way to make them OR. An activity meant to fire if either of two upstream activities fails must be split into two separate single-condition activities, not one activity with two dependency entries. Discovered when Complete Log Failure_Notebook's two-condition dependency silently never fired on a real Stage-activity failure (the log row stayed stuck at RUNNING); fixed by splitting into Complete Log Failure_Notebook_Stage and Complete Log Failure_Notebook_Load, each with exactly one dependency condition.
39. **NOTEBOOK_AZURE_SQL proven — the notebook itself needed zero changes**
  dbo.DimDate was cut over from EXECUTION_METHOD='PIPELINE' to 'NOTEBOOK', proving the Bronze NOTEBOOK path generalizes across source tech types, not just Fabric SQLDB. The Switch case (NOTEBOOK_AZURE_SQL) mirrors NOTEBOOK_FABRIC_SQLDB exactly — a "Stage AzureSQLDB Source" Copy activity (AzureSqlSource, not FabricSqlDatabaseSource) into the same ParquetSink/    Files pattern, feeding the same notebook. Critically, NB_Bronze_Staged_Ingestion itself required no changes at all — it only ever reads whatever Parquet was staged and writes Delta, with no awareness of which source connector produced the staged files. This is real validation that the stage-to-Parquet design (item #30) genuinely decouples the notebook from     source-specific concerns, not just for the one entity it was built against.  DimDate needed no SOURCE_QUERY_OVERRIDE (no known type-incompatibility issue) — proving the override is opt-in, not a mandatory step for every NOTEBOOK entity.
40. **Schema constraint discovered: EXECUTION_METHOD cutover must be an in-place UPDATE, never deactivate-old-insert-new**
    UIDX_META_ORCHESTRATION is unique on (TRIGGER_NAME, SOURCE_ENTITY_ID), and UIDX_META_CONFIGURATION_CORE is unique on (SOURCE_ENTITY_ID, CONFIGURATION_CATEGORY, CONFIGURATION_NAME) — neither includes IS_ACTIVEYN in the key. This means the schema hard-enforces exactly one orchestration row and one EXECUTION_METHOD config row per source entity, active or not; deactivating an old row does not free up the key for a new one. Discovered when migrating dbo.DimDate from PIPELINE to NOTEBOOK: an initial "deactivate old row, insert a new one" attempt failed on both Unique indexes. The correct, only-possible approach is an in-place UPDATE of the existing CONFIGURATION_VALUE (and, if ever needed, TARGET_ENTITY/    PROCESSING_METHOD/etc. on the same META_ORCHESTRATION row) — there is no "keep the old row as historical record" option under this schema. Rollback  is simply running the same UPDATE with the prior value.
41. **Silver Layer — v1 Feature Scope (Must-Have + Should-Have only)**
    Evaluated against the ISD Accelerator reference framework's full Curating-Data feature surface (merge types, transformation library, DQ rule engine, post-write reconciliation, surrogate keys, custom functions) and against industry-standard medallion architecture practice, to decide what actually earns the name "Silver" versus what's framework richness accumulated over
    time. Deliberately scoped down, same philosophy as the Gold placeholder (item #28) and skipping ISD's full Bronze engine (item #30) — build only what's needed now, document the rest as a known deferral rather than silently omitting it.

    MUST-HAVE (this is what makes it Silver, not optional):
    - Merge/upsert logic: both SCD1 (simple upsert — the default for most entities) and SCD2 (historized with scd_start_date/scd_end_date, starting with Person.Address, whose SCD2_SETTINGS metadata is already seeded per the project plan)
    - Deduplication: resolve to one row per primary key within a batch before merging, keeping the latest by watermark value
    - Primary-key uniqueness enforcement: a duplicate primary key after dedup is quarantined, not silently merged and not a whole-batch failure
    - Schema enforcement: target Delta table has a defined, typed schema;incoming data validated against it before merge
    - Basic cleansing only: blank-to-null, string trimming — explicitly not the full transformation library (see NOT NOW below)
    - Correct incremental extraction from Bronze, via the same watermark- column mechanism already proven in Bronze (see decision below — not Change Data Feed for v1)
    - Quarantine mechanism: rows failing validation are written to an actual quarantine table (inspectable later), not merely counted;  ROWS_REJECTED in META_INGESTION_LOG reflects the count
    - Auditability: reuses the existing META_INGESTION_LOG / spStart_Ingestion_Log / spComplete_Ingestion_Log pattern as-is — no new logging mechanism for Silver

    SHOULD-HAVE:
    - Post-write row-count reconciliation: compare rows merged vs. expected, logged at warn level, not blocking
    - Schema drift policy: auto-evolve via mergeSchema, same posture as Bronze — not a stricter fail-on-drift policy
    - Soft-delete propagation: capability documented now, not implemented until a real source with an actual delete-indicator column appears

    NOT NOW (explicitly deferred, not silently omitted):
    - Surrogate key generation and dimension-table attachment — industry- standard placement for this is Gold (Kimball dimensional modeling), not Silver; refines the existing Gold placeholder rather than introducing new Silver scope
    - The full data_transformation_steps library (pivot/unpivot, joins, window functions, aggregation, string/datetime functions, entity resolution, etc.)
    - The rich data_quality rule engine beyond basic PK/null checks (pattern validation, statistical anomaly detection, referential integrity, custom DQ functions)
    - PII masking
    - Custom-function escape hatches (custom_transformation_function,   
      custom_data_quality_function, etc.)
    - Execute-only delegated integrations (execute_warehouse_sp,
      execute_fabric_notebook, execute_fabric_dataflow)

    Two defaults chosen without a further round of confirmation, easy to override if wrong: (1) incremental extraction stays watermark-column- based, not Change Data Feed, since Bronze tables don't have CDF enabled today and enabling it retroactively is its own separate task, not a v1
    requirement; (2) quarantined rows physically land in a table, not just a count, since a count alone isn't inspectable later.

    Maps directly onto the project plan's existing Phase 4 tasks (4.1 SCD1, 4.2 SCD2, 4.3 quarantine via ROWS_REJECTED, 4.4 PL_Silver_Load wrapper, 4.5 post-write reconciliation, 4.6 E2E test) — this item formalizes what was already implicitly scoped there, rather than expanding it.
    


---

## 3. Data Model

### 3.1 Metadata Table
 
 
-- ============================================================
-- TABLE 1a: [DBO].[META_CONNECTION_ENDPOINT]
-- ONE ROW PER LOGICAL SOURCE/TARGET ENDPOINT.
-- Holds control-flow / structural fields the framework branches
-- on directly (auth type, endpoint type/tech type). Variable,
-- heterogeneous connection properties (host, port, path, secret
-- name, etc.) stay in the child table META_CONNECTION_PROPERTY ,
-- keyed back to this row via META_CONNECTION_ENDPOINT_ID.
-- ============================================================
CREATE TABLE [DBO].[META_CONNECTION_ENDPOINT](
    [META_CONNECTION_ENDPOINT_ID]   INT IDENTITY(1,1) NOT NULL,
    [DATA_ENDPOINT_NAME]            VARCHAR(500)  NOT NULL,   -- unique logical name, e.g. ORACLE_ERP_PROD
    [DATA_ENDPOINT_TYPE]            VARCHAR(50)   NOT NULL,   -- SOURCE / TARGET
    [DATA_ENDPOINT_TECH_TYPE]       VARCHAR(100)  NOT NULL,   -- ORACLE, AZURE_SQL, SFTP, FABRIC-LH, FABRIC-DW, REST_API ETC.
    [AUTH_TYPE]                     VARCHAR(30)   NULL,       -- controls which credential-resolution path the framework takes
    [GATEWAY_REQUIRED_YN]           CHAR(1)       NULL    DEFAULT 'N', -- Y = routed via on-prem/VNet data gateway
    [DESCRIPTION]                   VARCHAR(1000) NULL,
    [IS_ACTIVEYN]                   CHAR(1)       NULL    DEFAULT 'Y',
    [CREATED_BY]                    VARCHAR(100)  NULL,
    [CREATED_DT]                    DATETIME2     NULL    DEFAULT GETDATE(),
    [LAST_UPDATED_BY]               VARCHAR(100)  NULL,
    [LAST_UPDATED_DT]               DATETIME2     NULL,
 
    CONSTRAINT [PK_META_CONNECTION_ENDPOINT] PRIMARY KEY ([META_CONNECTION_ENDPOINT_ID]),
    CONSTRAINT [CHK_META_CONNECTION_ENDPOINT_TYPE] CHECK (
        [DATA_ENDPOINT_TYPE] IN ('SOURCE','TARGET')
    ),
    CONSTRAINT [CHK_META_CONNECTION_ENDPOINT_AUTH_TYPE] CHECK (
        [AUTH_TYPE] IS NULL
        OR [AUTH_TYPE] IN ('ENTRA_MANAGED_IDENTITY','SERVICE_PRINCIPAL','KEY_VAULT_SECRET','GATEWAY_SQL_AUTH')
    )
);
GO
 
CREATE UNIQUE INDEX [UIDX_META_CONNECTION_ENDPOINT_NAME]
    ON [DBO].[META_CONNECTION_ENDPOINT]([DATA_ENDPOINT_NAME]);
GO
 -- ============================================================
-- NEW TABLE: [DBO].[META_SOURCE_ENTITY]
-- MASTER TABLE — ONE ROW PER SOURCE ENTITY (table/file/object).
-- Owns SOURCE_ENTITY_ID (previously hand-assigned with no
-- source of truth) and links each entity to the SOURCE endpoint
-- it lives in — closing the gap that blocked the pipeline from
-- resolving a source connection.
-- ============================================================
CREATE TABLE [DBO].[META_SOURCE_ENTITY](
    [SOURCE_ENTITY_ID]                INT IDENTITY(1,1) NOT NULL,
    [SOURCE_ENTITY_NAME]              VARCHAR(500)  NOT NULL,   -- e.g. Person.Address
    [SOURCE_CONNECTION_ENDPOINT_ID]   INT           NOT NULL,   -- FK to META_CONNECTION_ENDPOINT (the SOURCE endpoint this entity lives in)
    [DESCRIPTION]                     VARCHAR(1000) NULL,
    [IS_ACTIVEYN]                     CHAR(1)       NULL DEFAULT 'Y',
    [CREATED_BY]                      VARCHAR(100)  NULL,
    [CREATED_DT]                      DATETIME2     NULL DEFAULT GETDATE(),
    [LAST_UPDATED_BY]                 VARCHAR(100)  NULL,
    [LAST_UPDATED_DT]                 DATETIME2     NULL,

    CONSTRAINT [PK_META_SOURCE_ENTITY] PRIMARY KEY ([SOURCE_ENTITY_ID]),
    CONSTRAINT [FK_META_SOURCE_ENTITY_ENDPOINT] FOREIGN KEY ([SOURCE_CONNECTION_ENDPOINT_ID])
        REFERENCES [DBO].[META_CONNECTION_ENDPOINT]([META_CONNECTION_ENDPOINT_ID])
);
GO

CREATE UNIQUE INDEX [UIDX_META_SOURCE_ENTITY_NAME]
    ON [DBO].[META_SOURCE_ENTITY]([SOURCE_ENTITY_NAME]);
GO

 
-- ============================================================
-- TABLE 1: [DBO].[META_CONNECTION_PROPERTY]
-- CHILD TABLE: ONE ROW PER CONNECTION PROPERTY, KEYED BACK TO
-- ITS PARENT ENDPOINT ROW. Property bag stays generic/EAV —
-- suitable for the heterogeneous, per-tech-type attribute sets
-- (host, port, path, secret name, API headers, etc.) that don't
-- need typed columns or constraints.
-- MODIFIED: DATA_ENDPOINT_TYPE / DATA_ENDPOINT_TECH_TYPE removed
-- from here — they now live once on the parent endpoint row,
-- not repeated on every property row.
-- ============================================================
CREATE TABLE [DBO].[META_CONNECTION_PROPERTY](
    [META_CONNECTION_PROPERTY_ID]   INT IDENTITY(1,1) NOT NULL,
    [META_CONNECTION_ENDPOINT_ID]   INT           NOT NULL,   -- FK to META_CONNECTION_ENDPOINT
    [PROPERTY_NAME]                 VARCHAR(500)  NOT NULL,
    [PROPERTY_VALUE]                VARCHAR(4000) NULL,
    [CREATED_BY]                    VARCHAR(100)  NULL,
    [CREATED_DT]                    DATETIME2     NULL    DEFAULT GETDATE(),
    [LAST_UPDATED_BY]               VARCHAR(100)  NULL,
    [LAST_UPDATED_DT]               DATETIME2     NULL,
    [IS_ACTIVEYN]                   CHAR(1)       NULL    DEFAULT 'Y',
 
    CONSTRAINT [PK_META_CONNECTION_PROPERTY] PRIMARY KEY ([META_CONNECTION_PROPERTY_ID]),
    CONSTRAINT [FK_META_CONNECTION_PROPERTY_ENDPOINT] FOREIGN KEY ([META_CONNECTION_ENDPOINT_ID])
        REFERENCES [DBO].[META_CONNECTION_ENDPOINT]([META_CONNECTION_ENDPOINT_ID])
);
 
 
CREATE UNIQUE INDEX [UIDX_META_CONNECTION_PROPERTY]
    ON [DBO].[META_CONNECTION_PROPERTY]([META_CONNECTION_ENDPOINT_ID], [PROPERTY_NAME]);
GO

 

-- ============================================================
-- TABLE 2: [DBO].[META_ORCHESTRATION]  
-- ONE ROW PER TRIGGER + SOURCE_ENTITY_ID
-- MODIFIED: EXECUTION_MODE / CUSTOM_NOTEBOOK_* replaced with a
-- generalized artifact-type pattern supporting extension via
-- STORED_PROCEDURE, NOTEBOOK, PIPELINE, or DATAFLOW_GEN2 —
-- while orchestration + logging always stay owned by the framework.
-- ============================================================
CREATE TABLE [DBO].[META_ORCHESTRATION](
    [META_ORCHESTRATION_ID]         INT IDENTITY(1,1) NOT NULL,
    [TRIGGER_NAME]                  VARCHAR(500)  NOT NULL,
    [SOURCE_ENTITY_ID]              INT           NOT NULL,
    [SOURCE_ENTITY_NAME]            VARCHAR(500)  NULL,
    [ORDER_OF_OPERATIONS]           INT           NULL,
    [META_CONNECTION_ENDPOINT_ID]   INT           NOT NULL,   -- FK to META_CONNECTION_ENDPOINT
    [TARGET_ENTITY]                 VARCHAR(4000) NULL,
    [PRIMARY_KEYS]                  VARCHAR(4000) NULL,
    [PROCESSING_METHOD]             VARCHAR(200)  NULL, 
    [IS_ACTIVEYN]                   CHAR(1)       NULL    DEFAULT 'Y',
    [CREATED_BY]                    VARCHAR(100)  NULL,
    [CREATED_DT]                    DATETIME2     NULL    DEFAULT GETDATE(),
    [LAST_UPDATED_BY]               VARCHAR(100)  NULL,
    [LAST_UPDATED_DT]               DATETIME2     NULL,
    -- ---- EXTENSIBILITY  ----
    [EXECUTION_ARTIFACT_TYPE]       VARCHAR(20)   NOT NULL DEFAULT 'FRAMEWORK',
        -- FRAMEWORK        : standard metadata-driven copy/load, no external artifact
        -- STORED_PROCEDURE : DB-native custom logic (e.g. complex SQL transform, merge)
        -- NOTEBOOK         : Fabric notebook (Spark/Python) built outside the framework
        -- PIPELINE         : another Fabric Data Factory pipeline
        -- DATAFLOW_GEN2    : Fabric Dataflow Gen2 (Power Query / low-code transform)
    [EXECUTION_WORKSPACE_ID]        VARCHAR(100)  NULL,   -- workspace GUID hosting the artifact (NULL = same workspace as framework)
    [EXECUTION_ARTIFACT_ID]         VARCHAR(1000) NULL,   -- notebook/pipeline/dataflow item GUID or path; stored proc name for STORED_PROCEDURE
    [EXECUTION_ARTIFACT_PARAMS]     VARCHAR(MAX)  NULL,   -- JSON: static params passed to the artifact at invocation time

    CONSTRAINT [PK_META_ORCHESTRATION] PRIMARY KEY ([META_ORCHESTRATION_ID]),
    CONSTRAINT [CHK_META_ORCHESTRATION_ARTIFACT_TYPE] CHECK (
        [EXECUTION_ARTIFACT_TYPE] IN ('FRAMEWORK','STORED_PROCEDURE','NOTEBOOK','PIPELINE','DATAFLOW_GEN2')
    ),
    -- Non-framework modes must identify what to invoke
    CONSTRAINT [CHK_META_ORCHESTRATION_ARTIFACT_ID] CHECK (
        [EXECUTION_ARTIFACT_TYPE] = 'FRAMEWORK' OR [EXECUTION_ARTIFACT_ID] IS NOT NULL
    ),
        CONSTRAINT [FK_META_ORCHESTRATION] FOREIGN KEY ([META_CONNECTION_ENDPOINT_ID])
        REFERENCES [DBO].[META_CONNECTION_ENDPOINT]([META_CONNECTION_ENDPOINT_ID])
);
 
GO

CREATE UNIQUE INDEX [UIDX_META_ORCHESTRATION] 
    ON [DBO].[META_ORCHESTRATION]([TRIGGER_NAME], [SOURCE_ENTITY_ID]);
GO
ALTER TABLE [DBO].[META_ORCHESTRATION]
    ADD CONSTRAINT [FK_META_ORCHESTRATION_SOURCE_ENTITY] FOREIGN KEY ([SOURCE_ENTITY_ID])
        REFERENCES [DBO].[META_SOURCE_ENTITY]([SOURCE_ENTITY_ID]);
GO

-- ============================================================
-- TABLE 3: [DBO].[META_CONFIGURATION_CORE]
 
-- ============================================================
CREATE TABLE [DBO].[META_CONFIGURATION_CORE](
    [META_CONFIGURATION_CORE_ID]    INT IDENTITY(1,1) NOT NULL,
    [SOURCE_ENTITY_ID]              INT           NOT NULL,
    [SOURCE_ENTITY_NAME]            VARCHAR(500)  NULL,
    [CONFIGURATION_CATEGORY]        VARCHAR(200)  NOT NULL,                                          
    [CONFIGURATION_NAME]            VARCHAR(200)  NOT NULL,
    [CONFIGURATION_VALUE]           VARCHAR(MAX)  NULL,
    [IS_ACTIVEYN]                   CHAR(1)       NULL    DEFAULT 'Y',
    [CREATED_BY]                    VARCHAR(100)  NULL,
    [CREATED_DT]                    DATETIME2     NULL    DEFAULT GETDATE(),
    [LAST_UPDATED_BY]               VARCHAR(100)  NULL,
    [LAST_UPDATED_DT]               DATETIME2     NULL,
    CONSTRAINT [PK_META_CONFIGURATION_CORE] PRIMARY KEY ([META_CONFIGURATION_CORE_ID])
);
GO

CREATE UNIQUE INDEX [UIDX_META_CONFIGURATION_CORE] 
    ON [DBO].[META_CONFIGURATION_CORE] ([SOURCE_ENTITY_ID], [CONFIGURATION_CATEGORY], [CONFIGURATION_NAME]);
GO

ALTER TABLE [DBO].[META_CONFIGURATION_CORE]
    ADD CONSTRAINT [FK_META_CONFIGURATION_CORE_SOURCE_ENTITY] FOREIGN KEY ([SOURCE_ENTITY_ID])
        REFERENCES [DBO].[META_SOURCE_ENTITY]([SOURCE_ENTITY_ID]);
GO

-- ============================================================
-- TABLE 4: [DBO].[META_CONFIGURATION_ADVANCED] 
 
-- ============================================================
CREATE TABLE [DBO].[META_CONFIGURATION_ADVANCED](
    [META_CONFIGURATION_ADVANCED_ID]        INT IDENTITY(1,1) NOT NULL,
    [SOURCE_ENTITY_ID]                      INT           NOT NULL,
    [SOURCE_ENTITY_NAME]                    VARCHAR(500)  NULL,
    [CONFIGURATION_CATEGORY]                VARCHAR(200)  NOT NULL,
    [CONFIGURATION_NAME]                    VARCHAR(200)  NOT NULL,
    [CONFIGURATION_NAME_INSTANCE_NUMBER]    INT           NOT NULL,
    [CONFIGURATION_ATTRIBUTE_NAME]          VARCHAR(200)  NOT NULL,
    [CONFIGURATION_ATTRIBUTE_VALUE]         VARCHAR(MAX)  NULL,
    [IS_ACTIVEYN]                           CHAR(1)       NULL    DEFAULT 'Y',
    [CREATED_BY]                            VARCHAR(100)  NULL,
    [CREATED_DT]                            DATETIME2     NULL    DEFAULT GETDATE(),
    [LAST_UPDATED_BY]                       VARCHAR(100)  NULL,
    [LAST_UPDATED_DT]                       DATETIME2     NULL,
    CONSTRAINT [PK_META_CONFIGURATION_ADVANCED] PRIMARY KEY ([META_CONFIGURATION_ADVANCED_ID])
);
GO

ALTER TABLE [DBO].[META_CONFIGURATION_ADVANCED]
    ADD CONSTRAINT [FK_META_CONFIGURATION_ADVANCED_SOURCE_ENTITY] FOREIGN KEY ([SOURCE_ENTITY_ID])
        REFERENCES [DBO].[META_SOURCE_ENTITY]([SOURCE_ENTITY_ID]);
GO
CREATE UNIQUE INDEX [UIDX_META_CONFIGURATION_ADVANCED]
    ON [DBO].[META_CONFIGURATION_ADVANCED] (
        [SOURCE_ENTITY_ID],
        [CONFIGURATION_CATEGORY],
        [CONFIGURATION_NAME],
        [CONFIGURATION_NAME_INSTANCE_NUMBER],
        [CONFIGURATION_ATTRIBUTE_NAME]
    );
GO

 


### 3.2 Log table

 

-- ============================================================
-- TABLE 5: [DBO].[META_INGESTION_LOG]
-- ONE ROW PER EXECUTION OF A META_ORCHESTRATION ENTRY
-- MODIFIED: mirrors EXECUTION_ARTIFACT_TYPE from orchestration and
-- adds ARTIFACT_RUN_ID so the framework can always capture which
-- external run (proc execution / notebook run / pipeline run /
-- dataflow job instance) a log row corresponds to — regardless of
-- artifact type, the FRAMEWORK remains the only writer to this table.
-- ============================================================
CREATE TABLE [DBO].[META_INGESTION_LOG](
    [META_INGESTION_LOG_ID]         BIGINT IDENTITY(1,1) NOT NULL,
    [META_ORCHESTRATION_ID]         INT           NOT NULL,   -- FK to META_ORCHESTRATION
    [SOURCE_ENTITY_ID]              INT           NOT NULL,
    [SOURCE_ENTITY_NAME]            VARCHAR(500)  NULL,
    [DATA_ENDPOINT_NAME]            VARCHAR(500)        NOT NULL,   -- Avoided FK for performance reason while logging
    [TARGET_ENTITY]                 VARCHAR(4000) NULL,
    [PROCESSING_METHOD]             VARCHAR(200)  NULL,       -- FULL / INCREMENTAL / CDC

    [TRIGGER_NAME]                  VARCHAR(500)  NULL,
    [TRIGGER_EXECUTION_ID]          VARCHAR(500)  NULL,       -- Fabric master pipeline run ID
    [TRIGGER_EXECUTION_START_TIME]  DATETIME2(6)  NULL,

    [RUN_START_DT]                  DATETIME2(6)  NULL,
    [RUN_END_DT]                    DATETIME2(6)  NULL,
    [RUN_STATUS]                    VARCHAR(20)   NOT NULL,   -- RUNNING / SUCCESS / FAILED / SKIPPED

    [WATERMARK_VALUE_USED]          VARCHAR(4000) NULL,       -- watermark this run started from
    [WATERMARK_VALUE_NEW]           VARCHAR(4000) NULL,       -- watermark to carry into next run

    [ROWS_READ]                     BIGINT        NULL,
    [ROWS_WRITTEN]                  BIGINT        NULL,
    [ROWS_REJECTED]                 BIGINT        NULL,

    [ERROR_MESSAGE]                 VARCHAR(MAX)  NULL,
    [FABRIC_MONITOR_URL]            VARCHAR(4000) NULL,

    -- ---- EXTENSIBILITY (replaces plain EXECUTION_MODE) ----
    [EXECUTION_ARTIFACT_TYPE]       VARCHAR(20)   NULL,       -- mirrors META_ORCHESTRATION.EXECUTION_ARTIFACT_TYPE
    [EXECUTION_ARTIFACT_ID]         VARCHAR(1000) NULL,       -- mirrors META_ORCHESTRATION.EXECUTION_ARTIFACT_ID at run time (captures value even if metadata changes later)
    [ARTIFACT_RUN_ID]               VARCHAR(500)  NULL,       -- proc execution context / notebook run ID / child pipeline RunId / dataflow job instance ID

    [CREATED_DT]                    DATETIME2(6)  NOT NULL DEFAULT SYSUTCDATETIME(),

    CONSTRAINT [PK_META_INGESTION_LOG] PRIMARY KEY ([META_INGESTION_LOG_ID]),
    CONSTRAINT [CHK_META_INGESTION_LOG_ARTIFACT_TYPE] CHECK (
        [EXECUTION_ARTIFACT_TYPE] IS NULL
        OR [EXECUTION_ARTIFACT_TYPE] IN ('FRAMEWORK','STORED_PROCEDURE','NOTEBOOK','PIPELINE','DATAFLOW_GEN2')
    ) 
);
GO

CREATE INDEX [NCI_META_INGESTION_LOG_ORCH]
    ON [DBO].[META_INGESTION_LOG]([META_ORCHESTRATION_ID], [RUN_START_DT] DESC);
GO

CREATE INDEX [NCI_META_INGESTION_LOG_EXEC]
    ON [DBO].[META_INGESTION_LOG]([TRIGGER_EXECUTION_ID]);
GO

CREATE INDEX [NCI_META_INGESTION_LOG_ARTIFACT]
    ON [DBO].[META_INGESTION_LOG]([EXECUTION_ARTIFACT_TYPE], [RUN_STATUS]);
GO
 

### 3.3 Sample source/target pair (for testing)

- Workspace1: IngestXcel - workspace for Metdata framwork metadata store (FabricSQL) and Target Lakehouse 
  Bronze and Silver lakehouses. 
- Workspace2: FabricTraining - Workspace act as the source. It has a SQLDB and we will take the table 
  from this database as source and populate the metadata table for development and testing. 
- FabricSQLDB -  IngestXcel SQLDB in IngestXcel workspace is the metadata database which support the framework
- FarbicSQLDB - Ingestxcel SQLDB in FabricTraining workspace act as the source. 
- Lakehouse: Bronze_LH in IngestXcel Workspace - For Bronze Layer
- Lakehouse: Silver_LH in IngestXcel Workspace - For Silver Layer

- Source: Ingestxcel SQLDB in FabricTraining Tables - Table Names
    [Person].[Address]
    [Person].[BusinessEntity]
    [Person].[EmailAddress]
- Target: Lakehouse Bronze Layer - Table Name - Same as the source 
- Load type: [full / incremental]
    [Person].[Address] - Incremental 
    [Person].[BusinessEntity] - Incremental
    [Person].[EmailAddress] - Full Load

Use this concrete pair when asking Claude to build or test new logic — prefer
testing against a real example over purely abstract code.

---

## 4. Non-Functional Requirements

- **Error handling:**one failed table in a Trigger_Name batch should not stop the whole batch. It   
   should continue and log the failure per row.  
- **Concurrency:** 4 sources/target run in parallel  
- **Retry policy:** max retries=3  - Retry policy: max retries = 3, configured as a per-activity property (General tab - Retry / Retry interval) on each pipeline activity that connects to an external source - not a metadata-table-driven setting. Applied to the Copy activity and any Lookup activities touching the source system; the metadata-store-only logging activities (Start Log, Complete Log Success/Failure) are lower priority for this since they're generally more stable.
- **Alerting:**  Fabric alerts, Teams, email

---

## 5. Conventions

- **Naming:** [table/column naming convention, notebook naming, pipeline  naming]
**TableName and Column Name :**   all Bronze (and future Silver/Gold) target-side schema names and 
    table names are lowercase (e.g. fabrictraining_ingestxcel, person_businessentitycontact), not uppercase as originally specified. This was changed after discovering that Spark/notebook-created objects are silently lowercased by default (see item #28) — rather than requiring every future table/schema creation to remember a special Spark config to preserve uppercase, the framework adopts lowercase as its standard so object creation works correctly by default, with no extra step to forget. 
    This applies to BRONZE_SCHEMA_ALIAS, TARGET_ENTITY, and their Silver/Gold equivalents once built. Source-side names (the actual external system's own schema/table names, e.g. Person.BusinessEntityContact in the source SQL DB) are untouched — this convention only governs names the framework itself creates in the target Lakehouse.
    Metadata columns/values unrelated to target object naming (e.g. PROCESSING_METHOD, EXECUTION_METHOD, AUTH_TYPE) keep their existing uppercase convention — this change is scoped specifically to physical schema/table names in the target, not metadata values generally.

- Trigger Naming Convention - TRG_{SYSTEM_IDENTIFIER}_{LAYER}_LOAD_{FREQUENCY}
    One trigger per source system, per layer, per schedule frequency — not one shared trigger across systems. This enables independent scheduling, pausing, and alerting per system (critical at the scale this framework targets — many independently-owned upstream systems, each with its own SLA and cadence), and allows individual tables within one system to be split across different frequency triggers if needed.
    Examples: TRG_FABRICTRAINING_INGESTXCEL_BRONZE_LOAD_DAILY, TRG_AZURESQL_ADVENTUREWORKS_SILVER_LOAD_HOURLY
    - Critical consistency rule: the {SYSTEM_IDENTIFIER} segment must exactly match that source's canonical identifier used everywhere else in metadata — specifically BRONZE_SCHEMA_ALIAS on the source's META_CONNECTION_ENDPOINT. One source system = one identifier, decided once at onboarding, reused consistently across trigger names, schema names, and any future source-facing labels. (E.g., the existing FabricTraining source uses FABRICTRAINING_INGESTXCEL consistently as both its schema alias and its trigger-name segment.)
    - PL_Master_Orchestrator derives Bronze/Silver/Gold trigger names from two parameters (SystemIdentifier, Frequency) rather than hardcoding full trigger name strings — each system+frequency combination gets its own independent Fabric schedule trigger invoking the orchestrator with just those two values.
- **Folder structure (Git-synced repo):** [describe layout — e.g.
  `/notebooks`, `/pipelines`, `/sql`, `/docs`]
- **Branching strategy:** Follow the industry best practices
- **Code style:** Following the microsoft or industry best practices
- **Language:** Go for best suitable library which supports most of the features 
- **Stored procedure naming**: sp<Verb>_<Entity> (e.g. spGet_Bronze_Batch) — never the sp_ prefix, which forces SQL Server to check master first on every call and risks colliding with a system procedure name.
---
## 6. How to Work With Me on This Project

- Build in small, reviewable units — not "the whole framework" in one pass.
  Suggested breakdown: (1) log table DDL + write helper, (2) connection
  resolver module, (3) generic copy/load function, (4) orchestrator that reads
  metadata and loops over active rows, (5) error handling/logging wrapper.
- Before generating code, restate which of the Section 2 non-negotiables apply
  to the current task.
- Flag any assumption you're making explicitly rather than silently picking
  one — especially around auth type, load type, or error handling for a new
  source type not yet covered in Section 3.
- If a request conflicts with a decision in Section 2, say so before
  proceeding rather than implementing the conflict.

 
