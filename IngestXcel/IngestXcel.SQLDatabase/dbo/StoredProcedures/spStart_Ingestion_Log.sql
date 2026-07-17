/* ============================================================
   spStart_Ingestion_Log
   Inserts a RUNNING row into META_INGESTION_LOG for one entity's
   execution and returns the new META_INGESTION_LOG_ID via OUTPUT
   so the pipeline can carry it through to the success/failure
   completion step later.

   Deliberately takes only META_ORCHESTRATION_ID + the trigger's
   execution ID as input — everything else (source entity info,
   target entity, processing method, execution artifact type)
   is looked up fresh from META_ORCHESTRATION itself, rather than
   passed in from the pipeline's earlier Lookup output. This keeps
   the pipeline's Script activity call simple (2 parameters) and
   guarantees the log row reflects the actual current metadata
   row, not a possibly-stale copy carried through pipeline
   variables.
--Test Script 
DECLARE @LogId BIGINT;
EXEC [DBO].[spStart_Ingestion_Log]
    @META_ORCHESTRATION_ID = 1,           -- use a real ID from your seeded Bronze rows
    @TriggerExecutionId = 'test-run-001',
    @META_INGESTION_LOG_ID = @LogId OUTPUT;
SELECT @LogId AS NewLogId;

SELECT * FROM [DBO].[META_INGESTION_LOG] WHERE META_INGESTION_LOG_ID = @LogId;


   ============================================================ */

CREATE   PROCEDURE [DBO].[spStart_Ingestion_Log]
    @META_ORCHESTRATION_ID       INT,
    @TriggerExecutionId          VARCHAR(500),
    @TriggerExecutionStartTime   DATETIME2(6)  = NULL,
    @META_INGESTION_LOG_ID       BIGINT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @SourceEntityId       INT;
    DECLARE @SourceEntityName     VARCHAR(500);
    DECLARE @TargetEntity         VARCHAR(4000);
    DECLARE @ProcessingMethod     VARCHAR(200);
    DECLARE @TriggerName          VARCHAR(500);
    DECLARE @ExecutionArtifactType VARCHAR(20);
    DECLARE @ExecutionArtifactId  VARCHAR(1000);
    DECLARE @DataEndpointName     VARCHAR(500);  -- target endpoint's name, paired with TARGET_ENTITY

    SELECT
        @SourceEntityId        = O.SOURCE_ENTITY_ID,
        @SourceEntityName      = O.SOURCE_ENTITY_NAME,
        @TargetEntity          = O.TARGET_ENTITY,
        @ProcessingMethod      = O.PROCESSING_METHOD,
        @TriggerName           = O.TRIGGER_NAME,
        @ExecutionArtifactType = O.EXECUTION_ARTIFACT_TYPE,
        @ExecutionArtifactId   = O.EXECUTION_ARTIFACT_ID,
        @DataEndpointName      = TGT_EP.DATA_ENDPOINT_NAME
    FROM [DBO].[META_ORCHESTRATION] O
    INNER JOIN [DBO].[META_CONNECTION_ENDPOINT] TGT_EP
        ON TGT_EP.META_CONNECTION_ENDPOINT_ID = O.META_CONNECTION_ENDPOINT_ID
    WHERE O.META_ORCHESTRATION_ID = @META_ORCHESTRATION_ID;

    IF @SourceEntityId IS NULL
    BEGIN
        RAISERROR('sp_Start_Ingestion_Log: META_ORCHESTRATION_ID %d not found or inactive.', 16, 1, @META_ORCHESTRATION_ID);
        RETURN;
    END

    INSERT INTO [DBO].[META_INGESTION_LOG]
        ([META_ORCHESTRATION_ID], [SOURCE_ENTITY_ID], [SOURCE_ENTITY_NAME], [DATA_ENDPOINT_NAME],
         [TARGET_ENTITY], [PROCESSING_METHOD], [TRIGGER_NAME], [TRIGGER_EXECUTION_ID],
         [TRIGGER_EXECUTION_START_TIME], [RUN_START_DT], [RUN_STATUS],
         [EXECUTION_ARTIFACT_TYPE], [EXECUTION_ARTIFACT_ID])
    VALUES
        (@META_ORCHESTRATION_ID, @SourceEntityId, @SourceEntityName, @DataEndpointName,
         @TargetEntity, @ProcessingMethod, @TriggerName, @TriggerExecutionId,
         ISNULL(@TriggerExecutionStartTime, SYSUTCDATETIME()), SYSUTCDATETIME(), 'RUNNING',
         @ExecutionArtifactType, @ExecutionArtifactId);

    SET @META_INGESTION_LOG_ID = SCOPE_IDENTITY();
END

GO

