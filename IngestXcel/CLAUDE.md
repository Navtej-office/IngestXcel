# CLAUDE.md — Metadata-Driven Ingestion Framework (Microsoft Fabric)
> Last updated: 2026-07-15

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
   Schema evolution is not automatic by default; entities requiring drift-safety use a notebook-based load path (see item #20), configured explicitly per entity.
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
18. **Execution and Orchestration Requirements**  Each medallion layer (Bronze, Silver, Gold) has its 
    own trigger name and its own set of META_ORCHESTRATION rows (e.g. TRG_BRONZE_LOAD_DAILY, TRG_SILVER_LOAD_DAILY, TRG_GOLD_LOAD_DAILY). Layer sequencing is enforced at the pipeline level: a master orchestrator pipeline invokes the Bronze, Silver, and Gold pipelines in strict order using Execute Pipeline activities with waitOnCompletion = true, so Silver never starts before Bronze completes, and Gold never starts before Silver completes. Within each layer, multiple source systems are processed in parallel (grouped by source connection endpoint), and multiple tables within the same source system run in parallel subject to ORDER_OF_OPERATIONS. This requires no additional metadata schema — the existing TRIGGER_NAME per layer is sufficient.
19. **Schedule Frequency Support** The metadata-driven ingestion framework shall support native fabric 
     scheduling /trigger feature and support  minimum scheduling frequency of once per hour. The framework shall also be designed to accommodate lower-frequency schedules (e.g., daily, weekly, or monthly)  
20. **Onboarding Governance Model-Layer Schema/Table Provisioning**
    Target schema and table(s) for all three layers (Bronze, Silver, Gold) are provisioned as a one-time manual setup step during consumer onboarding/change registration — never auto-created by any pipeline or notebook at runtime. This applies uniformly across layers; Silver and Gold structures (SCD tracking columns, business-logic outputs) cannot be mechanically inferred from source schema in the first place, making explicit DDL a hard requirement there, not merely a preference carried over from Bronze.
    The framework is owned and governed by a central team. Consumers request onboarding via the governance forum and submit their metadata registration using the framework's specified template.
    As part of registration, the consumer (or the central team on their behalf) creates the schema and target table(s) for every layer being registered, matching exactly what's declared in metadata (TARGET_ENTITY, BRONZE_SCHEMA_ALIAS, and their Silver/Gold equivalents once those layers are built).
    All Copy/write activities use "Use existing", never auto-create. A run failing on a missing schema/table signals an incomplete registration, not a pipeline defect — and keeps the pipeline's own execution identity scoped to write/insert permissions only, with no CREATE TABLE/CREATE SCHEMA rights needed anywhere.
    Schema drift is not silently auto-applied at any layer. Tested directly: Fabric's Copy activity, writing to an existing Delta table via implicit name-based mapping, neither errors nor adds a genuinely new source column — it's silently dropped. This confirmed the need for an explicit per-entity choice rather than relying on default Copy activity behavior.
    Bronze schema-drift handling (open, to be finalized in a follow-up build phase): a per-entity flag — META_CONFIGURATION_CORE (CONFIGURATION_CATEGORY='BRONZE', CONFIGURATION_NAME='SCHEMA_EVOLUTION_REQUIRED', Y/N) — will determine whether an entity's Bronze load uses the standard pipeline Copy activity (cheap, no drift handling) or a notebook-based write using Delta's mergeSchema option (handles new source columns automatically, at Spark compute cost). This flag will be a required, explicit field in the onboarding registration template — never silently defaulted — so the central team makes an informed per-table call on expected schema stability.
    Silver/Gold schema drift: by design, changes do not need to automatically propagate beyond Bronze. A new source column landing in Bronze is sufficient; whether and how it flows into Silver/Gold is a deliberate, separate change managed through the same registration/approval process — not automatic.
21. **Schema-per-source-system** the Bronze schema a table lands under is resolved from the source 
    endpoint, not the target Lakehouse — stored as BRONZE_SCHEMA_ALIAS on the source's META_CONNECTION_ENDPOINT (e.g. FABRICTRAINING_INGESTXCEL). This prevents two distinct collision scenarios when a single Bronze Lakehouse hosts multiple source systems: two sources sharing a schema/table name, and one source having two schemas with an identically-named table (the latter already handled separately by keeping target table names schema-prefixed, e.g. PERSON_ADDRESS vs SALES_ADDRESS).  
22. **Connection Design** One Fabric Connection per physical source database, not one universal connection per tech type.     
    Fabric's "SQL database" connector binds a Connection object to one specific database at creation time — it does not expose separate dynamic Workspace/Database override fields the way the Lakehouse connector does. The Connection's own GUID is stored as a FABRIC_CONNECTION_ID property on the source endpoint, and the Copy activity's Connection field is set to "Use dynamic content" referencing that stored ID. Adding a new physical source database requires creating one new Connection and recording its ID in metadata — no pipeline changes.
23. **Bronze growth management:** Bronze remains append-only by design (never truncated). Growth is controlled via (1) 
    partitioning     by INGESTION_BATCH_DT, (2) a retention/purge policy — default [confirm: 180 days] — enforced by a scheduled maintenance job, not the ingestion pipeline itself, and (3) governance review at onboarding to ensure PROCESSING_METHOD = FULL is only assigned to small, low-volume reference tables. Not yet implemented — tracked as a follow-up build phase alongside schema-drift handling.
24. **Fabric Constraints**
    (a) The lowercase-GUID rule. WORKSPACE_ID/LAKEHOUSE_ID/SQLDATABASE_ID/FABRIC_CONNECTION_ID must be stored lowercase — OneLake's path validation is case-sensitive and rejects uppercase, even though a GUID is technically case-insensitive. 
    (b) The Lakehouse schema-naming rule. Confirmed directly from Fabric's own error message: schema names must contain only letters, numbers, and underscores — no hyphens.  
    (c) The Lakehouse SQL analytics endpoint is read-only. No DDL or data writes (CREATE SCHEMA, CREATE TABLE, INSERT/UPDATE/DELETE) can be executed against it. Schema/table creation and all data modifications must go through the Lakehouse Explorer UI or a Spark notebook.
    (d) Activities nested inside a Switch case or If Condition branch cannot be referenced from outside that construct. Bridge required values across scope via pipeline variables, set immediately after the nested activity — safe only because the ForEach is Sequential, not Parallel.

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
- **Retry policy:** max retries=3 
- **Alerting:**  Fabric alerts, Teams, email

---

## 5. Conventions

- **Naming:** [table/column naming convention, notebook naming, pipeline  naming]
**TableName and Column Name :** Keep the table and column  name always upper case in target  and any table or column related to metadata. 

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

 