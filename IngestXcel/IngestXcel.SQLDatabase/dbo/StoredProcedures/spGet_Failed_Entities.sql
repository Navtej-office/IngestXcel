/* ============================================================
   Name        : spGet_Failed_Entities
   Purpose     : Returns a ready-to-paste, comma-separated list
                 of META_ORCHESTRATION_ID values that failed in
                 the MOST RECENT execution of a given trigger —
                 designed to be copied straight into the
                 TargetEntityIds pipeline parameter for a
                 surgical restart, without re-processing entities
                 that already succeeded (critical for FULL-load
                 entities, which would otherwise get a redundant
                 duplicate append on every retry).
   Parameters  : @TriggerName VARCHAR(500)
   Returns     : Exactly one row:
                   FAILED_ENTITY_IDS   - comma-separated
                                         META_ORCHESTRATION_ID list,
                                         '' if nothing failed or the
                                         trigger has never run
                   FAILED_ENTITY_NAMES - comma-separated
                                         SOURCE_ENTITY_NAME list,
                                         for human-readable review
                                         alongside the ID list
   Notes       : "Most recent execution" is determined by the
                 latest TRIGGER_EXECUTION_ID for this trigger
                 (i.e. the latest pipeline run), not just the
                 latest RUN_START_DT per entity — this avoids
                 mixing failures from two different runs together.
                 Never returns NULL — an empty string means
                 "nothing to restart," matching TargetEntityIds'
                 own "empty = process everything" convention on
                 the next normal (non-restart) run.
   ============================================================ */

CREATE   PROCEDURE [DBO].[spGet_Failed_Entities]
    @TriggerName VARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @LatestTriggerExecutionId VARCHAR(500) = (
        SELECT TOP 1 TRIGGER_EXECUTION_ID
        FROM [DBO].[META_INGESTION_LOG]
        WHERE TRIGGER_NAME = @TriggerName
        ORDER BY RUN_START_DT DESC
    );

    SELECT
        COALESCE(STRING_AGG(CAST(META_ORCHESTRATION_ID AS VARCHAR), ','), '') AS FAILED_ENTITY_IDS,
        COALESCE(STRING_AGG(SOURCE_ENTITY_NAME, ', '), '')                    AS FAILED_ENTITY_NAMES
    FROM [DBO].[META_INGESTION_LOG]
    WHERE TRIGGER_NAME         = @TriggerName
      AND TRIGGER_EXECUTION_ID = @LatestTriggerExecutionId
      AND RUN_STATUS           = 'FAILED';
END

GO

