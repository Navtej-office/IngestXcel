/* ============================================================
   Name        : spGet_Last_Watermark
   Purpose     : Returns the watermark value an incremental
                 entity's next run should start from — the
                 WATERMARK_VALUE_NEW from its most recent
                 SUCCESSFUL run, or a floor default if no prior
                 successful run exists (first-ever run, or every
                 prior attempt failed).
   Parameters  : @META_ORCHESTRATION_ID INT
                 @DefaultWatermark VARCHAR(4000) — optional,
                 defaults to '1900-01-01'. Assumes a date-like
                 watermark column; revisit if a non-date
                 watermark (numeric/GUID) is ever added.
   Returns     : Exactly one row, always (via COALESCE over a
                 scalar subquery), so a Lookup activity with
                 "First row only" behaves predictably whether or
                 not run history exists yet.
   Notes       : Called only for PROCESSING_METHOD = 'INCREMENTAL'
                 entities — full-load entities never call this.



/* ============================================================
   TEST SCRIPT — spGet_Last_Watermark
   Expected (no prior successful run): WATERMARK_VALUE = '1900-01-01'
   ============================================================ */

--EXEC [DBO].[spGet_Last_Watermark] @META_ORCHESTRATION_ID = 1;

-- To verify the "has history" path, insert a fake successful run
-- and re-run the EXEC above (delete the test row afterward):
--
-- INSERT INTO [DBO].[META_INGESTION_LOG]
--     (META_ORCHESTRATION_ID, SOURCE_ENTITY_ID, SOURCE_ENTITY_NAME, DATA_ENDPOINT_NAME,
--      TARGET_ENTITY, PROCESSING_METHOD, RUN_STATUS, RUN_START_DT, RUN_END_DT, WATERMARK_VALUE_NEW)
-- VALUES
--     (1, 1, 'Person.Address', 'TGT_INGESTXCEL_BRONZE_LH',
--      'PERSON_ADDRESS', 'INCREMENTAL', 'SUCCESS', SYSUTCDATETIME(), SYSUTCDATETIME(), '2026-07-01 00:00:00');


   ============================================================ */

CREATE   PROCEDURE [DBO].[spGet_Last_Watermark]
    @META_ORCHESTRATION_ID INT,
    @DefaultWatermark      VARCHAR(4000) = '1900-01-01'
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        COALESCE(
            (
                SELECT TOP 1 L.WATERMARK_VALUE_NEW
                FROM [DBO].[META_INGESTION_LOG] L
                WHERE L.META_ORCHESTRATION_ID = @META_ORCHESTRATION_ID
                  AND L.RUN_STATUS = 'SUCCESS'
                ORDER BY L.RUN_END_DT DESC
            ),
            @DefaultWatermark
        ) AS WATERMARK_VALUE;
END

GO

