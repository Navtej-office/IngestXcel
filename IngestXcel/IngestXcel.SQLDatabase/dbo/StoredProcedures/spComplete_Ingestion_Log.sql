/* ============================================================
   spComplete_Ingestion_Log
   Updates a RUNNING log row to its terminal state. Called from
   both the pipeline's success branch (Status='SUCCESS', row
   counts, new watermark) and failure branch (Status='FAILED',
   error message) after the Copy activity finishes.

   Key correctness rule: WATERMARK_VALUE_NEW is only written when
   @Status = 'SUCCESS'. On failure, the watermark is left
   untouched — so the next run retries from the same starting
   point rather than silently skipping the data that failed to
   load. This is what makes incremental loads idempotent across
   a failed-then-retried run.

--Test script 
DECLARE @LogId BIGINT;

EXEC [DBO].[spStart_Ingestion_Log]
    @META_ORCHESTRATION_ID = 1,
    @TriggerExecutionId = 'test-run-002',
    @META_INGESTION_LOG_ID = @LogId OUTPUT;

-- Simulate a successful Copy activity
EXEC [DBO].[spComplete_Ingestion_Log]
    @META_INGESTION_LOG_ID = @LogId,
    @Status = 'SUCCESS',
    @RowsRead = 150,
    @RowsWritten = 150,
    @WatermarkValueUsed = '1900-01-01',
    @WatermarkValueNew = '2026-07-16 10:00:00';

SELECT * FROM [DBO].[META_INGESTION_LOG] WHERE META_INGESTION_LOG_ID = @LogId;

   ============================================================ */

CREATE   PROCEDURE [DBO].[spComplete_Ingestion_Log]
    @META_INGESTION_LOG_ID   BIGINT,
    @Status                  VARCHAR(20),        -- SUCCESS / FAILED / SKIPPED
    @RowsRead                BIGINT        = NULL,
    @RowsWritten             BIGINT        = NULL,
    @RowsRejected            BIGINT        = NULL,
    @WatermarkValueUsed      VARCHAR(4000) = NULL,
    @WatermarkValueNew       VARCHAR(4000) = NULL,
    @ErrorMessage            VARCHAR(MAX)  = NULL,
    @ArtifactRunId           VARCHAR(500)  = NULL,
    @FabricMonitorUrl        VARCHAR(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    IF @Status NOT IN ('SUCCESS','FAILED','SKIPPED')
    BEGIN
        RAISERROR('spComplete_Ingestion_Log: invalid @Status ''%s''. Must be SUCCESS, FAILED, or SKIPPED.', 16, 1, @Status);
        RETURN;
    END

    UPDATE [DBO].[META_INGESTION_LOG]
    SET
        RUN_STATUS           = @Status,
        RUN_END_DT           = SYSUTCDATETIME(),
        ROWS_READ            = @RowsRead,
        ROWS_WRITTEN         = @RowsWritten,
        ROWS_REJECTED        = @RowsRejected,
        WATERMARK_VALUE_USED = ISNULL(@WatermarkValueUsed, WATERMARK_VALUE_USED),
        WATERMARK_VALUE_NEW  = CASE WHEN @Status = 'SUCCESS' THEN @WatermarkValueNew ELSE WATERMARK_VALUE_NEW END,
        ERROR_MESSAGE        = @ErrorMessage,
        ARTIFACT_RUN_ID      = ISNULL(@ArtifactRunId, ARTIFACT_RUN_ID),
        FABRIC_MONITOR_URL   = ISNULL(@FabricMonitorUrl, FABRIC_MONITOR_URL)
    WHERE META_INGESTION_LOG_ID = @META_INGESTION_LOG_ID;

    IF @@ROWCOUNT = 0
    BEGIN
        RAISERROR('spComplete_Ingestion_Log: META_INGESTION_LOG_ID %I64d not found.', 16, 1, @META_INGESTION_LOG_ID);
    END
END

GO

