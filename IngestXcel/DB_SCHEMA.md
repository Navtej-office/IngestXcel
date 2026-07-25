# Metadata Database Schema

DDL for the metadata store (Fabric SQL Database, `IngestXcel` workspace) that drives this
framework — one row per source entity, connection endpoint, orchestration entry, config value,
and log entry. Moved out of `CLAUDE.md` into its own file since it's reference material, not a
decision log — a schema change is a code change, tracked here directly rather than narrated.

For the *decisions behind* this shape (why an EAV property table, why extensibility columns on
`META_ORCHESTRATION`, etc.), see the relevant ADR in `docs/decisions/`.

---

## Metadata Tables

 
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

