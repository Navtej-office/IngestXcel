-- =============================================================================
-- Procedure:    spGet_Silver_Batch
-- Layer:        Silver
-- Purpose:      Returns the batch of Silver entities to process for a given
--               trigger, mirroring spGet_Bronze_Batch's shape and conventions
--               exactly. Feeds the ForEach loop in PL_Silver_Load.
--
-- Key design point: the "source" for every Silver entity is Bronze's own
-- Lakehouse table, not the original external system. Per ISD Accelerator's
-- confirmed convention (each layer transition gets its own orchestration
-- row — a Bronze load and a Silver load for the same business entity are
-- two separate META_ORCHESTRATION_ID rows, never one shared row), each
-- Silver entity has its own META_SOURCE_ENTITY row (e.g. 'bronze.person_
-- address') whose SOURCE_CONNECTION_ENDPOINT_ID points at Bronze's own
-- TARGET endpoint (TGT_INGESTXCEL_BRONZE_LH) — reused as a source
-- reference, no new Fabric Connection needed.
--
-- Returned columns, beyond the common set (META_ORCHESTRATION_ID,
-- SOURCE_ENTITY_ID, SOURCE_ENTITY_NAME, ORDER_OF_OPERATIONS, TARGET_ENTITY,
-- PRIMARY_KEYS, PROCESSING_METHOD, resolved source/target workspace+
-- lakehouse IDs):
--   WATERMARK_COLUMN        - column used to filter Bronze rows already
--                              processed into Silver (same mechanism as
--                              Bronze's own watermark against the original
--                              source, just one layer further down)
--   SCD_TYPE                - 'SCD1' (simple upsert) or 'SCD2' (historized)
--   SCD2_START_DATE_COL     - NULL for SCD1 entities. For SCD2, reuses the
--                              same natural business timestamp as
--                              WATERMARK_COLUMN (e.g. ModifiedDate) rather
--                              than a separate system-generated timestamp —
--                              simplest defensible choice, per the Silver
--                              v1 minimal-scope decision (CLAUDE.md #41)
--   SCD2_END_DATE_COL       - NULL for SCD1. New column the Silver merge
--                              maintains on the target table (e.g.
--                              SCD_END_DATE), NULL while a row is current
--   SCD2_CURRENT_FLAG_COL   - NULL for SCD1. New column the Silver merge
--                              maintains (e.g. IS_CURRENT)
--   QUARANTINE_TABLE_NAME   - target for rows failing basic PK/null
--                              validation (Silver v1 quarantine mechanism)
--
-- Deliberately NOT included, per a design discussion that reconsidered an
-- earlier, never-applied draft: no separate DEDUP_KEY field — PRIMARY_KEYS
-- is reused for deduplication, since no current entity needs the two to
-- differ. No META_CONFIGURATION_ADVANCED usage — SCD2 settings are always
-- exactly one fixed set per entity, so three flat META_CONFIGURATION_CORE
-- rows are enough; ADVANCED's instance-numbered EAV pattern is reserved for
-- config that genuinely repeats, which this doesn't.
--
-- Parameters:
--   @TriggerName      - required. Which Silver trigger group to process
--                        (e.g. 'TRG_SILVER_LOAD_DAILY').
--   @TargetEntityIds  - optional, comma-separated META_ORCHESTRATION_IDs.
--                        Empty string (default) processes every active
--                        entity under @TriggerName. Same restart/rerun
--                        mechanism as spGet_Bronze_Batch (CLAUDE.md #27).
--
-- Filters applied: O.IS_ACTIVEYN / SRC_EP.IS_ACTIVEYN / TGT_EP.IS_ACTIVEYN
-- all = 'Y', and EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK' (excludes any
-- delegated execute-only rows, same convention as Bronze).
--
-- =============================================================================
-- TEST SCRIPT (commented out — uncomment to run manually)
-- =============================================================================
--
-- -- 1) Confirm the proc returns exactly one row for Person.Address, with
-- --    SCD_TYPE = 'SCD2' and all three SCD2_* columns populated:
-- EXEC spGet_Silver_Batch @TriggerName = 'TRG_SILVER_LOAD_DAILY';
--
-- -- 2) Confirm scoping to a single entity works (replace <ID> with the
-- --    META_ORCHESTRATION_ID printed by test 1):
-- EXEC spGet_Silver_Batch @TriggerName = 'TRG_SILVER_LOAD_DAILY', @TargetEntityIds = '<ID>';
--
-- -- 3) Confirm an unmatched trigger name returns zero rows, not an error:
-- EXEC spGet_Silver_Batch @TriggerName = 'TRG_DOES_NOT_EXIST';
--
-- =============================================================================

CREATE     PROCEDURE [dbo].[spGet_Silver_Batch]
    @TriggerName VARCHAR(500),
    @TargetEntityIds VARCHAR(4000) = ''
AS
BEGIN
    SET NOCOUNT ON;
    SET @TargetEntityIds = ISNULL(@TargetEntityIds, '');

    ;WITH SourceProps AS (
        SELECT
            META_CONNECTION_ENDPOINT_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'WORKSPACE_ID' THEN PROPERTY_VALUE END) AS SOURCE_WORKSPACE_ID,
            MAX(CASE WHEN PROPERTY_NAME = 'LAKEHOUSE_ID' THEN PROPERTY_VALUE END) AS SOURCE_LAKEHOUSE_ID
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

        LOWER(SP.SOURCE_WORKSPACE_ID)  AS SOURCE_WORKSPACE_ID,
        LOWER(SP.SOURCE_LAKEHOUSE_ID)  AS SOURCE_LAKEHOUSE_ID,
        BSA.PROPERTY_VALUE             AS SOURCE_SCHEMA_NAME,
        LOWER(TP.TARGET_WORKSPACE_ID)  AS TARGET_WORKSPACE_ID,
        LOWER(TP.TARGET_LAKEHOUSE_ID)  AS TARGET_LAKEHOUSE_ID,

        WM.CONFIGURATION_VALUE   AS WATERMARK_COLUMN,
        ST.CONFIGURATION_VALUE   AS SCD_TYPE,
        S2S.CONFIGURATION_VALUE  AS SCD2_START_DATE_COL,
        S2E.CONFIGURATION_VALUE  AS SCD2_END_DATE_COL,
        S2C.CONFIGURATION_VALUE  AS SCD2_CURRENT_FLAG_COL,
        QTN.CONFIGURATION_VALUE  AS QUARANTINE_TABLE_NAME

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
 LEFT JOIN [DBO].[META_ORCHESTRATION] BO
        ON BO.TARGET_ENTITY = O.SOURCE_ENTITY_NAME
        AND BO.IS_ACTIVEYN = 'Y' AND BO.EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK'
        AND BO.META_CONNECTION_ENDPOINT_ID = (
            SELECT META_CONNECTION_ENDPOINT_ID FROM [DBO].[META_CONNECTION_ENDPOINT]
            WHERE DATA_ENDPOINT_NAME = 'TGT_INGESTXCEL_BRONZE_LH'
        )
    LEFT JOIN [DBO].[META_SOURCE_ENTITY] BSE
        ON BSE.SOURCE_ENTITY_ID = BO.SOURCE_ENTITY_ID
    LEFT JOIN [DBO].[META_CONNECTION_PROPERTY] BSA
        ON BSA.META_CONNECTION_ENDPOINT_ID = BSE.SOURCE_CONNECTION_ENDPOINT_ID
        AND BSA.PROPERTY_NAME = 'BRONZE_SCHEMA_ALIAS' AND BSA.IS_ACTIVEYN = 'Y'
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] WM
        ON WM.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND WM.CONFIGURATION_CATEGORY = 'SILVER'
        AND WM.CONFIGURATION_NAME = 'WATERMARK_COLUMN' AND WM.IS_ACTIVEYN = 'Y'
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] ST
        ON ST.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND ST.CONFIGURATION_CATEGORY = 'SILVER'
        AND ST.CONFIGURATION_NAME = 'SCD_TYPE' AND ST.IS_ACTIVEYN = 'Y'
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] S2S
        ON S2S.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND S2S.CONFIGURATION_CATEGORY = 'SILVER'
        AND S2S.CONFIGURATION_NAME = 'SCD2_START_DATE_COL' AND S2S.IS_ACTIVEYN = 'Y'
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] S2E
        ON S2E.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND S2E.CONFIGURATION_CATEGORY = 'SILVER'
        AND S2E.CONFIGURATION_NAME = 'SCD2_END_DATE_COL' AND S2E.IS_ACTIVEYN = 'Y'
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] S2C
        ON S2C.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND S2C.CONFIGURATION_CATEGORY = 'SILVER'
        AND S2C.CONFIGURATION_NAME = 'SCD2_CURRENT_FLAG_COL' AND S2C.IS_ACTIVEYN = 'Y'
    LEFT JOIN [DBO].[META_CONFIGURATION_CORE] QTN
        ON QTN.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND QTN.CONFIGURATION_CATEGORY = 'SILVER'
        AND QTN.CONFIGURATION_NAME = 'QUARANTINE_TABLE_NAME' AND QTN.IS_ACTIVEYN = 'Y'

    WHERE O.TRIGGER_NAME          = @TriggerName
      AND O.IS_ACTIVEYN           = 'Y'
      AND SRC_EP.IS_ACTIVEYN      = 'Y'
      AND TGT_EP.IS_ACTIVEYN      = 'Y'
      AND O.EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK'
      AND (@TargetEntityIds = '' OR O.META_ORCHESTRATION_ID IN (
            SELECT CAST(value AS INT) FROM STRING_SPLIT(@TargetEntityIds, ',')
          ))
    ORDER BY O.ORDER_OF_OPERATIONS;
END

GO

