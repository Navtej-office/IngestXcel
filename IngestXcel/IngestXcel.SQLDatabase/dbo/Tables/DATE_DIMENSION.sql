CREATE TABLE [dbo].[DATE_DIMENSION] (
    [DATE_KEY]          INT           NOT NULL,
    [CALENDAR_DATE]     DATE          NOT NULL,
    [DATE_TEXT]         VARCHAR (20)  NOT NULL,
    [YEAR_NUM]          INT           NOT NULL,
    [QUARTER_NUM]       INT           NOT NULL,
    [MONTH_NUM]         INT           NOT NULL,
    [MONTH_NAME]        VARCHAR (20)  NOT NULL,
    [MONTH_NAME_ABBREV] VARCHAR (10)  NOT NULL,
    [DAY_NUM]           INT           NOT NULL,
    [DAY_NAME]          VARCHAR (20)  NOT NULL,
    [DAY_OF_WEEK]       INT           NOT NULL,
    [WEEK_OF_YEAR]      INT           NOT NULL,
    [IS_WEEKEND]        BIT           NOT NULL,
    [MONTH_YEAR]        VARCHAR (20)  NOT NULL,
    [SORT_YEAR]         INT           NOT NULL,
    [SORT_QUARTER]      INT           NOT NULL,
    [SORT_MONTH]        INT           NOT NULL,
    [SORT_DAY]          INT           NOT NULL,
    [SORT_DAY_OF_WEEK]  INT           NOT NULL,
    [SORT_WEEK_OF_YEAR] INT           NOT NULL,
    [SORT_YEAR_MONTH]   INT           NOT NULL,
    [CREATED_DT]        DATETIME2 (6) DEFAULT (sysutcdatetime()) NOT NULL,
    CONSTRAINT [PK_DATE_DIMENSION] PRIMARY KEY CLUSTERED ([DATE_KEY] ASC)
);


GO

