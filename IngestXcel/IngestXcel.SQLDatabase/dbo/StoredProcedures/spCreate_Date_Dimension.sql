/* ============================================================
   spCreate_Date_Dimension
   One-time populate of DATE_DIMENSION for Power BI reporting.
   Idempotent: no-ops if DATE_DIMENSION already has rows, so it's
   safe to call from a setup/deployment step every time.

   Range: 2020-01-01 through 2035-12-31 (5,844 days, well inside the
   4,096-per-block generator below, produced via two blocks). Adjust
   @StartDate/@EndDate if you need a different window -- there's no
   dependency elsewhere on these specific bounds.

   --Test script
   EXEC [dbo].[spCreate_Date_Dimension];
   SELECT COUNT(*) AS Days, MIN(CALENDAR_DATE) AS FirstDay, MAX(CALENDAR_DATE) AS LastDay
   FROM [dbo].[DATE_DIMENSION];
   ============================================================ */

CREATE PROCEDURE [dbo].[spCreate_Date_Dimension]
AS
BEGIN
    SET NOCOUNT ON;

    IF EXISTS (SELECT 1 FROM [dbo].[DATE_DIMENSION])
    BEGIN
        RETURN;
    END

    DECLARE @StartDate DATE = '2020-01-01';
    DECLARE @EndDate   DATE = '2035-12-31';

    -- Generate a numbers series via cross-joined VALUES to build all dates in a single
    -- set-based INSERT (no recursive CTE, no WHILE loop). 4 x 4-row sets -> 256
    -- combinations, cross-joined with 256 -> 65,536 rows max, comfortably covering the
    -- ~5,844-day range above.
    ;WITH
    N1(n)   AS (SELECT n FROM (VALUES (0),(1),(2),(3)) t(n)),
    N2(n)   AS (SELECT a.n FROM N1 a CROSS JOIN N1 b),                -- 16
    N3(n)   AS (SELECT a.n FROM N2 a CROSS JOIN N2 b),                -- 256
    Nums    AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 AS num FROM N3 a CROSS JOIN N3 b), -- 65,536
    Dates   AS (
        SELECT DATEADD(DAY, num, @StartDate) AS CalendarDate
        FROM Nums
        WHERE DATEADD(DAY, num, @StartDate) <= @EndDate
    )
    INSERT INTO [dbo].[DATE_DIMENSION]
        ([DATE_KEY], [CALENDAR_DATE], [DATE_TEXT], [YEAR_NUM], [QUARTER_NUM], [MONTH_NUM],
         [MONTH_NAME], [MONTH_NAME_ABBREV], [DAY_NUM], [DAY_NAME], [DAY_OF_WEEK],
         [WEEK_OF_YEAR], [IS_WEEKEND], [MONTH_YEAR],
         [SORT_YEAR], [SORT_QUARTER], [SORT_MONTH], [SORT_DAY], [SORT_DAY_OF_WEEK],
         [SORT_WEEK_OF_YEAR], [SORT_YEAR_MONTH])
    SELECT
        CONVERT(INT, CONVERT(VARCHAR(8), CalendarDate, 112))                      AS DATE_KEY,
        CalendarDate                                                              AS CALENDAR_DATE,
        CONVERT(VARCHAR(20), CalendarDate, 23)                                    AS DATE_TEXT,
        YEAR(CalendarDate)                                                        AS YEAR_NUM,
        DATEPART(QUARTER, CalendarDate)                                          AS QUARTER_NUM,
        MONTH(CalendarDate)                                                       AS MONTH_NUM,
        DATENAME(MONTH, CalendarDate)                                             AS MONTH_NAME,
        LEFT(DATENAME(MONTH, CalendarDate), 3)                                    AS MONTH_NAME_ABBREV,
        DAY(CalendarDate)                                                         AS DAY_NUM,
        DATENAME(WEEKDAY, CalendarDate)                                           AS DAY_NAME,
        DATEPART(WEEKDAY, CalendarDate)                                           AS DAY_OF_WEEK,
        DATEPART(WEEK, CalendarDate)                                              AS WEEK_OF_YEAR,
        CASE WHEN DATEPART(WEEKDAY, CalendarDate) IN (1, 7) THEN 1 ELSE 0 END     AS IS_WEEKEND,
        CONCAT(LEFT(DATENAME(MONTH, CalendarDate), 3), ' ', YEAR(CalendarDate))   AS MONTH_YEAR,
        YEAR(CalendarDate)                                                        AS SORT_YEAR,
        YEAR(CalendarDate) * 10 + DATEPART(QUARTER, CalendarDate)                 AS SORT_QUARTER,
        YEAR(CalendarDate) * 100 + MONTH(CalendarDate)                            AS SORT_MONTH,
        CONVERT(INT, CONVERT(VARCHAR(8), CalendarDate, 112))                      AS SORT_DAY,
        DATEPART(WEEKDAY, CalendarDate)                                           AS SORT_DAY_OF_WEEK,
        YEAR(CalendarDate) * 100 + DATEPART(WEEK, CalendarDate)                   AS SORT_WEEK_OF_YEAR,
        YEAR(CalendarDate) * 100 + MONTH(CalendarDate)                            AS SORT_YEAR_MONTH
    FROM Dates;

END

GO

