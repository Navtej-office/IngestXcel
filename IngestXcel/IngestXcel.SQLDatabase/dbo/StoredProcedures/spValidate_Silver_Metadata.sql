/* ============================================================
   Name        : spValidate_Silver_Metadata
   Purpose     : Pre-flight check for a Silver trigger batch —
                 mirrors spValidate_Bronze_Metadata's shape and
                 style exactly, flagging orchestration rows with
                 missing or invalid Silver-specific metadata
                 before you ever run the pipeline against them.
   Parameters  : @TriggerName VARCHAR(500)
   Returns     : One row per active entity under the trigger, with
                 VALIDATION_STATUS ('OK' or 'ISSUES_FOUND') and
                 VALIDATION_ISSUES (semicolon-separated list of
                 problems, NULL if none).
   Checks      : SCD_TYPE present and exactly 'SCD1' or 'SCD2';
                 if SCD2, all three SCD2_* columns present;
                 WATERMARK_COLUMN present; QUARANTINE_TABLE_NAME
                 present; TARGET_ENTITY/PRIMARY_KEYS present;
                 PROCESSING_METHOD valid; and — Silver-specific —
                 the Bronze table this entity reads from
                 (SOURCE_ENTITY_NAME, e.g. 'bronze.person_address')
                 still has an ACTIVE Bronze orchestration row.
                 A Silver entity pointed at a deactivated or
                 renamed Bronze table is a silent data gap, not an
                 error anywhere else, so it's cheap to catch here.
   Notes       : Only checks EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK'
                 rows, same convention as Bronze. No DEDUP_KEY or
                 META_CONFIGURATION_ADVANCED checks — deliberately
                 not part of the design (see spGet_Silver_Batch's
                 header for why).
   ============================================================ */

CREATE    PROCEDURE [dbo].[spValidate_Silver_Metadata]
    @TriggerName VARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    ;WITH Checked AS (
        SELECT
            O.META_ORCHESTRATION_ID,
            O.SOURCE_ENTITY_NAME,
            O.TARGET_ENTITY,
            O.PROCESSING_METHOD,
            ST.CONFIGURATION_VALUE  AS SCD_TYPE,
            WM.CONFIGURATION_VALUE  AS WATERMARK_COLUMN,
            S2S.CONFIGURATION_VALUE AS SCD2_START_DATE_COL,
            S2E.CONFIGURATION_VALUE AS SCD2_END_DATE_COL,
            S2C.CONFIGURATION_VALUE AS SCD2_CURRENT_FLAG_COL,
            QTN.CONFIGURATION_VALUE AS QUARANTINE_TABLE_NAME,

            -- Does an active Bronze orchestration row exist for the
            -- table this Silver entity reads from? SOURCE_ENTITY_NAME
            -- is expected in the form 'bronze.<table>' — strip the
            -- prefix and check against Bronze's TARGET_ENTITY.
            (
                SELECT COUNT(1)
                FROM [DBO].[META_ORCHESTRATION] BO
                WHERE BO.IS_ACTIVEYN = 'Y'
                  AND BO.EXECUTION_ARTIFACT_TYPE = 'FRAMEWORK'
                  AND BO.TARGET_ENTITY = SUBSTRING(O.SOURCE_ENTITY_NAME, LEN('bronze.') + 1, 4000)
            ) AS ActiveBronzeSourceCount,

            CONCAT(
                CASE WHEN O.TARGET_ENTITY IS NULL OR O.TARGET_ENTITY = '' THEN 'Missing TARGET_ENTITY; ' END,
                CASE WHEN O.PRIMARY_KEYS IS NULL OR O.PRIMARY_KEYS = '' THEN 'Missing PRIMARY_KEYS; ' END,
                CASE WHEN O.PROCESSING_METHOD NOT IN ('FULL','INCREMENTAL')
                     THEN CONCAT('Invalid PROCESSING_METHOD ''', O.PROCESSING_METHOD, ''''';') END,
                CASE WHEN WM.CONFIGURATION_VALUE IS NULL THEN 'Missing SILVER WATERMARK_COLUMN config; ' END,
                CASE WHEN QTN.CONFIGURATION_VALUE IS NULL THEN 'Missing SILVER QUARANTINE_TABLE_NAME config; ' END,
                CASE WHEN ST.CONFIGURATION_VALUE IS NULL
                     THEN 'Missing required SILVER SCD_TYPE; ' END,
                CASE WHEN ST.CONFIGURATION_VALUE IS NOT NULL AND ST.CONFIGURATION_VALUE NOT IN ('SCD1','SCD2')
                     THEN CONCAT('Invalid SCD_TYPE ''', ST.CONFIGURATION_VALUE, ''''';') END,
                CASE WHEN ST.CONFIGURATION_VALUE = 'SCD2' AND S2S.CONFIGURATION_VALUE IS NULL
                     THEN 'SCD2 entity missing SCD2_START_DATE_COL; ' END,
                CASE WHEN ST.CONFIGURATION_VALUE = 'SCD2' AND S2E.CONFIGURATION_VALUE IS NULL
                     THEN 'SCD2 entity missing SCD2_END_DATE_COL; ' END,
                CASE WHEN ST.CONFIGURATION_VALUE = 'SCD2' AND S2C.CONFIGURATION_VALUE IS NULL
                     THEN 'SCD2 entity missing SCD2_CURRENT_FLAG_COL; ' END
            ) AS BaseValidationIssues

        FROM [DBO].[META_ORCHESTRATION] O
        INNER JOIN [DBO].[META_SOURCE_ENTITY] SE
            ON SE.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID
        INNER JOIN [DBO].[META_CONNECTION_ENDPOINT] SRC_EP
            ON SRC_EP.META_CONNECTION_ENDPOINT_ID = SE.SOURCE_CONNECTION_ENDPOINT_ID
        INNER JOIN [DBO].[META_CONNECTION_ENDPOINT] TGT_EP
            ON TGT_EP.META_CONNECTION_ENDPOINT_ID = O.META_CONNECTION_ENDPOINT_ID
        LEFT JOIN [DBO].[META_CONFIGURATION_CORE] ST
            ON ST.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND ST.CONFIGURATION_CATEGORY = 'SILVER'
            AND ST.CONFIGURATION_NAME = 'SCD_TYPE' AND ST.IS_ACTIVEYN = 'Y'
        LEFT JOIN [DBO].[META_CONFIGURATION_CORE] WM
            ON WM.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID AND WM.CONFIGURATION_CATEGORY = 'SILVER'
            AND WM.CONFIGURATION_NAME = 'WATERMARK_COLUMN' AND WM.IS_ACTIVEYN = 'Y'
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
    )
    SELECT
        META_ORCHESTRATION_ID,
        SOURCE_ENTITY_NAME,
        TARGET_ENTITY,
        PROCESSING_METHOD,
        SCD_TYPE,
        CASE WHEN NULLIF(
            CONCAT(
                BaseValidationIssues,
                CASE WHEN ActiveBronzeSourceCount = 0
                     THEN CONCAT('Source Bronze table for ''', SOURCE_ENTITY_NAME, ''' has no active orchestration row; ') END
            ), '') IS NULL THEN 'OK' ELSE 'ISSUES_FOUND' END AS VALIDATION_STATUS,
        NULLIF(
            CONCAT(
                BaseValidationIssues,
                CASE WHEN ActiveBronzeSourceCount = 0
                     THEN CONCAT('Source Bronze table for ''', SOURCE_ENTITY_NAME, ''' has no active orchestration row; ') END
            ), '') AS VALIDATION_ISSUES
    FROM Checked
    ORDER BY VALIDATION_STATUS DESC, SOURCE_ENTITY_NAME;
END

GO

