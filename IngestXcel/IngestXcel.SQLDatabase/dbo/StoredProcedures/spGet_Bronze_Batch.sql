/* ============================================================
   Name        : spGet_Bronze_Batch
   Purpose     : Returns one flat row per active Bronze entity
                 for a given trigger — source connection details,
                 target connection details, and the Bronze
                 watermark column (if incremental) all pivoted
                 into named columns, ready for a pipeline Lookup
                 activity to consume via item().<column_name>.
   Parameters  : @TriggerName VARCHAR(500) — e.g. 'TRG_BRONZE_LOAD_DAILY'
   Returns     : One row per active FRAMEWORK-mode orchestration
                 row under that trigger, ordered by ORDER_OF_OPERATIONS.
   Notes       : Only returns EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK'
                 rows — extensibility rows (STORED_PROCEDURE/
                 NOTEBOOK/PIPELINE/DATAFLOW_GEN2) are handled
                 elsewhere, not by this standard Bronze copy path.
   ============================================================ */

CREATE   PROCEDURE [DBO].[spGet_Bronze_Batch]
    @TriggerName VARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH SourceProps AS (
        SELECT
            META_CONNECTION_ENDPOINT_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'SQL_SERVER_HOST' THEN PROPERTY_VALUE END) AS SQL_SERVER_HOST,
            MAX(CASE WHEN PROPERTY_NAME = 'SQL_SERVER_PORT' THEN PROPERTY_VALUE END) AS SQL_SERVER_PORT,
            MAX(CASE WHEN PROPERTY_NAME = 'DATABASE_NAME'   THEN PROPERTY_VALUE END) AS SOURCE_DATABASE_NAME,
            MAX(CASE WHEN PROPERTY_NAME = 'FABRIC_CONNECTION_ID' THEN PROPERTY_VALUE END) AS SOURCE_FABRIC_CONNECTION_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'WORKSPACE_ID' THEN PROPERTY_VALUE END) AS SOURCE_WORKSPACE_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'SQLDATABASE_ID' THEN PROPERTY_VALUE END) AS SOURCE_SQLDATABASE_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'BRONZE_SCHEMA_ALIAS' THEN PROPERTY_VALUE END) AS TARGET_SCHEMA_NAME
        FROM [DBO].[META_CONNECTION_PROPERTY]
        WHERE IS_ACTIVEYN = 'Y'
        GROUP BY META_CONNECTION_ENDPOINT_ID
    ),
    TargetProps AS (
        SELECT
            META_CONNECTION_ENDPOINT_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'WORKSPACE_ID' THEN PROPERTY_VALUE END) AS TARGET_WORKSPACE_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'LAKEHOUSE_ID' THEN PROPERTY_VALUE END) AS TARGET_LAKEHOUSE_ID
        FROM [DBO].[META_CONNECTION_PROPERTY]
        WHERE IS_ACTIVEYN = 'Y'
        GROUP BY META_CONNECTION_ENDPOINT_ID
    )
    SELECT
        O.META_ORCHESTRATION_ID,
        O.SOURCE_ENTITY_ID,
        O.SOURCE_ENTITY_NAME,
        O.ORDER_OF_OPERATIONS,
        O.TARGET_ENTITY,
        O.PRIMARY_KEYS,
        O.PROCESSING_METHOD,

        SRC_EP.META_CONNECTION_ENDPOINT_ID AS SOURCE_CONNECTION_ENDPOINT_ID,
        SRC_EP.DATA_ENDPOINT_NAME          AS SOURCE_ENDPOINT_NAME,
        SRC_EP.DATA_ENDPOINT_TECH_TYPE     AS SOURCE_TECH_TYPE,
        SRC_EP.AUTH_TYPE                   AS SOURCE_AUTH_TYPE,
        SRC_EP.GATEWAY_REQUIRED_YN         AS SOURCE_GATEWAY_REQUIRED_YN,
        SP.SQL_SERVER_HOST,
        SP.SQL_SERVER_PORT,
        SP.SOURCE_DATABASE_NAME,
        SP.SOURCE_FABRIC_CONNECTION_ID,
        LOWER(SP.SOURCE_WORKSPACE_ID)   AS SOURCE_WORKSPACE_ID,   -- OneLake path validation is case-sensitive and rejects uppercase GUID chars, even though a GUID is case-insensitive by definition
        LOWER(SP.SOURCE_SQLDATABASE_ID) AS SOURCE_SQLDATABASE_ID, -- same reason — protects against a future source registered with an uppercase-pasted GUID
        SP.TARGET_SCHEMA_NAME,  -- Bronze schema is keyed off the SOURCE system, not the target Lakehouse, to avoid cross-source table-name collisions

        TGT_EP.META_CONNECTION_ENDPOINT_ID AS TARGET_CONNECTION_ENDPOINT_ID,
        TGT_EP.DATA_ENDPOINT_NAME          AS TARGET_ENDPOINT_NAME,
        TGT_EP.AUTH_TYPE                   AS TARGET_AUTH_TYPE,
        LOWER(TP.TARGET_WORKSPACE_ID)  AS TARGET_WORKSPACE_ID,  -- root cause of a real production bug: an uppercase Workspace GUID here made the Copy activity's OneLake "filesystem" path validation fail outright
        LOWER(TP.TARGET_LAKEHOUSE_ID)  AS TARGET_LAKEHOUSE_ID,  -- same OneLake path validation rule applies to the Lakehouse ID segment

        WM.CONFIGURATION_VALUE AS WATERMARK_COLUMN

    FROM [DBO].[META_ORCHESTRATION] O
    INNER JOIN [DBO].[META_SOURCE_ENTITY] SE
        ON SE.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID
    INNER JOIN [DBO].[META_CONNECTION_ENDPOINT] SRC_EP
        ON SRC_EP.META_CONNECTION_ENDPOINT_ID = SE.SOURCE_CONNECTION_ENDPOINT_ID
    LEFT JOIN SourceProps SP
        ON SP.META_CONNECTION_ENDPOINT_ID = SRC_EP.META_CONNECTION_ENDPOINT_ID
    INNER JOIN [DBO].[META_CONNECTION_ENDPOINT] TGT_EP
        ON TGT_EP.META_CONNECTION_ENDPOINT_ID = O.META_CONNECTION_ENDPOINT_ID
    LEFT JOIN TargetProps TP
        ON TP.META_CONNECTION_ENDPOINT_ID = TGT_EP.META_CONNECTION_ENDPOINT_ID
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] WM
        ON WM.SOURCE_ENTITY_ID       = O.SOURCE_ENTITY_ID
        AND WM.CONFIGURATION_CATEGORY = 'BRONZE'
        AND WM.CONFIGURATION_NAME     = 'WATERMARK_COLUMN'
        AND WM.IS_ACTIVEYN            = 'Y'

    WHERE O.TRIGGER_NAME          = @TriggerName
      AND O.IS_ACTIVEYN           = 'Y'
      AND SRC_EP.IS_ACTIVEYN      = 'Y'
      AND TGT_EP.IS_ACTIVEYN      = 'Y'
      AND O.EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK'
    ORDER BY O.ORDER_OF_OPERATIONS;
END

GO

