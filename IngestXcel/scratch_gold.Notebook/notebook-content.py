# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c798f7f7-f235-4ec9-a90a-cc9489233d65",
# META       "default_lakehouse_name": "Gold_LH",
# META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
# META       "known_lakehouses": [
# META         {
# META           "id": "c798f7f7-f235-4ec9-a90a-cc9489233d65"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# =============================================================================
# Script:   08_gold_table_ddl.py
# Purpose:  Create all Gold tables in Gold_LH: 3 SCD2 dimensions, 1 static
#           date dimension, 1 transactional fact, and 5 matching quarantine
#           tables. Matches exactly what 06/07a/07b registered in metadata.
#
# Run in:   A Fabric NOTEBOOK (PySpark), with Gold_LH set as its default
#           lakehouse -- NOT the SQL analytics endpoint. Gold_LH is a
#           Lakehouse, so its SQL endpoint is read-only; DDL has to go
#           through Spark, same as Bronze_LH/Silver_LH always have.
#           Paste this into a new cell in a new (or scratch) notebook
#           attached to Gold_LH and run it once.
#
# Design assumptions made here (flagging rather than silently deciding):
#   - Source housekeeping columns (rowguid, ModifiedDate) are dropped from
#     every dimension -- Gold already has its own SCD_START_DATE/audit
#     timestamps, so ModifiedDate would be redundant, and rowguid has no
#     business/analytical value. All other Silver business columns are
#     carried through unchanged.
#   - Money-like decimal columns use DECIMAL(19,4) -- the diagnostic query
#     didn't return NUMERIC_PRECISION/SCALE, so this is a safe generic
#     default rather than a verified exact match to Silver's precision.
#   - dimsalesterritory carries SalesYTD/SalesLastYear/CostYTD/CostLastYear
#     as-is from Silver, reusing the same hash-compare SCD2 algorithm
#     ADR-0010 specifies. WORTH FLAGGING: these are running-total financial
#     columns that can change often, which means dimsalesterritory could
#     accumulate many SCD2 versions over time (a "rapidly changing
#     attribute" problem in Kimball terms). Not redesigned here since
#     ADR-0010 didn't scope specific columns out of the hash-compare -- say
#     the word if you'd rather exclude these 4 columns from the dimension
#     (or from what triggers a new SCD2 version) and I'll adjust.
#   - Quarantine tables mirror each entity's raw pre-processing columns
#     (natural key + business attributes) -- NO surrogate key, NO SCD2
#     bookkeeping, NO added reason/timestamp/log-ID columns. This matches
#     Silver's actual proven pattern (confirmed by reading NB_Silver_SCD_
#     Load.Notebook live): a quarantined row is just new_data.filter(...)
#     written as-is, before any SCD/SK logic runs -- "raw Bronze-shape
#     columns only, no SCD_START_DATE/SCD_END_DATE/IS_CURRENT" is the
#     established, ISD-Accelerator-informed convention this project has
#     already proven at Silver. Gold's quarantine tables apply the same
#     principle one layer down: raw Silver-read shape (dimensions) / raw
#     joined natural-key shape (fact), nothing added. Kept DDL-first
#     (explicit CREATE TABLE, not lazy-create) since that's this project's
#     own later, superseding governance decision for quarantine tables.
#   - dimdate does NOT get SCD2 columns (ADR-0010 flagged this as an open
#     question -- "happy to force SCD2 columns onto it anyway for schema
#     uniformity if you'd rather" -- defaulting to no, since it was never
#     confirmed). Still gets INSERT_DATETIME/UPDATE_DATETIME/
#     SOURCE_INGESTION_LOG_ID for structural consistency with the other
#     Gold tables, even though the latter will always be NULL for dimdate
#     (it's generated outside the META_INGESTION_LOG-driven pipeline).
#
# UPDATED after reviewing the reference accelerator's real code: every
# dimension (3 sourced + dimdate) is now seeded with an UNKNOWN-MEMBER row,
# surrogate key = -1, matching its proven `_attach_dimension_surrogate_key`
# pattern (LEFT JOIN + COALESCE unmatched fact FK to -1, never reject the
# fact row for a missing dimension). This replaces v1's originally-planned
# "quarantine the fact row if its dimension key isn't found yet" behavior --
# see ADR-0010's revised Late-arriving dimension fallback decision.
# factsalesorderdetail's FK columns are now NOT NULL as a result: they
# always resolve to either a real surrogate key or -1, never blank.
# Quarantine tables' SCOPE narrowed to match: only a fact/dimension row
# whose OWN natural key is null gets quarantined now (exactly Silver's
# proven single-condition convention) -- missing-dimension cases no longer
# quarantine at all, so this file's earlier "worth flagging" comment about
# reconciling quarantine shape stands, but the TRIGGER condition changed.
# =============================================================================

# -----------------------------------------------------------------------------
# DIMPRODUCT
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dimproduct (
    PRODUCT_SK             BIGINT      NOT NULL,
    ProductID              INT         NOT NULL,
    Name                    STRING,
    ProductNumber          STRING,
    MakeFlag               STRING,
    FinishedGoodsFlag      STRING,
    Color                  STRING,
    SafetyStockLevel       SMALLINT,
    ReorderPoint           SMALLINT,
    StandardCost           DECIMAL(19,4),
    ListPrice              DECIMAL(19,4),
    Size                    STRING,
    SizeUnitMeasureCode    STRING,
    WeightUnitMeasureCode  STRING,
    Weight                  DECIMAL(19,4),
    DaysToManufacture      INT,
    ProductLine             STRING,
    Class                   STRING,
    Style                   STRING,
    ProductSubcategoryID   INT,
    ProductModelID         INT,
    SellStartDate           TIMESTAMP,
    SellEndDate             TIMESTAMP,
    DiscontinuedDate        TIMESTAMP,
    SCD_START_DATE          TIMESTAMP   NOT NULL,
    SCD_END_DATE             TIMESTAMP,
    IS_CURRENT                STRING    NOT NULL,
    INSERT_DATETIME           TIMESTAMP NOT NULL,
    UPDATE_DATETIME           TIMESTAMP,
    SOURCE_INGESTION_LOG_ID   BIGINT
) USING DELTA
""")

if spark.sql("SELECT COUNT(*) AS c FROM dimproduct WHERE PRODUCT_SK = -1").collect()[0]["c"] == 0:
    spark.sql("""
    INSERT INTO dimproduct (PRODUCT_SK, ProductID, Name, ProductNumber, SCD_START_DATE, IS_CURRENT, INSERT_DATETIME)
    VALUES (-1, -1, 'Unknown', 'UNKNOWN', current_timestamp(), 'Y', current_timestamp())
    """)

spark.sql("""
CREATE TABLE IF NOT EXISTS dimproduct_quarantine (
    ProductID              INT,
    Name                    STRING,
    ProductNumber          STRING,
    MakeFlag                STRING,
    FinishedGoodsFlag       STRING,
    Color                   STRING,
    SafetyStockLevel        SMALLINT,
    ReorderPoint             SMALLINT,
    StandardCost             DECIMAL(19,4),
    ListPrice                 DECIMAL(19,4),
    Size                       STRING,
    SizeUnitMeasureCode        STRING,
    WeightUnitMeasureCode       STRING,
    Weight                       DECIMAL(19,4),
    DaysToManufacture             INT,
    ProductLine                    STRING,
    Class                           STRING,
    Style                            STRING,
    ProductSubcategoryID              INT,
    ProductModelID                     INT,
    SellStartDate                       TIMESTAMP,
    SellEndDate                          TIMESTAMP,
    DiscontinuedDate                      TIMESTAMP
) USING DELTA
""")

# -----------------------------------------------------------------------------
# DIMCUSTOMER
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dimcustomer (
    CUSTOMER_SK               BIGINT      NOT NULL,
    CustomerID                INT         NOT NULL,
    PersonID                  INT,
    StoreID                   INT,
    TerritoryID                INT,
    AccountNumber              STRING,
    SCD_START_DATE              TIMESTAMP  NOT NULL,
    SCD_END_DATE                 TIMESTAMP,
    IS_CURRENT                     STRING  NOT NULL,
    INSERT_DATETIME                TIMESTAMP NOT NULL,
    UPDATE_DATETIME                TIMESTAMP,
    SOURCE_INGESTION_LOG_ID        BIGINT
) USING DELTA
""")

if spark.sql("SELECT COUNT(*) AS c FROM dimcustomer WHERE CUSTOMER_SK = -1").collect()[0]["c"] == 0:
    spark.sql("""
    INSERT INTO dimcustomer (CUSTOMER_SK, CustomerID, AccountNumber, SCD_START_DATE, IS_CURRENT, INSERT_DATETIME)
    VALUES (-1, -1, 'UNKNOWN', current_timestamp(), 'Y', current_timestamp())
    """)

spark.sql("""
CREATE TABLE IF NOT EXISTS dimcustomer_quarantine (
    CustomerID       INT,
    PersonID          INT,
    StoreID            INT,
    TerritoryID          INT,
    AccountNumber         STRING
) USING DELTA
""")

# -----------------------------------------------------------------------------
# DIMSALESTERRITORY
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dimsalesterritory (
    SALESTERRITORY_SK        BIGINT       NOT NULL,
    TerritoryID               INT          NOT NULL,
    Name                       STRING,
    CountryRegionCode          STRING,
    SalesTerritoryGroup        STRING,
    SalesYTD                    DECIMAL(19,4),
    SalesLastYear                DECIMAL(19,4),
    CostYTD                       DECIMAL(19,4),
    CostLastYear                   DECIMAL(19,4),
    SCD_START_DATE                  TIMESTAMP NOT NULL,
    SCD_END_DATE                     TIMESTAMP,
    IS_CURRENT                        STRING  NOT NULL,
    INSERT_DATETIME                   TIMESTAMP NOT NULL,
    UPDATE_DATETIME                   TIMESTAMP,
    SOURCE_INGESTION_LOG_ID           BIGINT
) USING DELTA
""")

if spark.sql("SELECT COUNT(*) AS c FROM dimsalesterritory WHERE SALESTERRITORY_SK = -1").collect()[0]["c"] == 0:
    spark.sql("""
    INSERT INTO dimsalesterritory (SALESTERRITORY_SK, TerritoryID, Name, SCD_START_DATE, IS_CURRENT, INSERT_DATETIME)
    VALUES (-1, -1, 'Unknown', current_timestamp(), 'Y', current_timestamp())
    """)

spark.sql("""
CREATE TABLE IF NOT EXISTS dimsalesterritory_quarantine (
    TerritoryID           INT,
    Name                    STRING,
    CountryRegionCode        STRING,
    SalesTerritoryGroup       STRING,
    SalesYTD                   DECIMAL(19,4),
    SalesLastYear                DECIMAL(19,4),
    CostYTD                       DECIMAL(19,4),
    CostLastYear                    DECIMAL(19,4)
) USING DELTA
""")

# -----------------------------------------------------------------------------
# DIMDATE -- static, one-off, not driven by META_SOURCE_ENTITY/watermark
# machinery, no SCD2 columns. DATE_SK is a smart key (YYYYMMDD as INT) so it
# doubles as the natural key -- no separate MAX(SK)+row_number() step needed
# the way the sourced dimensions require, since this is generated in one shot.
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS dimdate (
    DATE_SK                   INT         NOT NULL,
    CALENDAR_DATE               DATE,
    YEAR                          INT,
    QUARTER                       INT,
    MONTH                         INT,
    MONTH_NAME                    STRING,
    DAY                           INT,
    DAY_OF_WEEK                   INT,
    DAY_NAME                      STRING,
    WEEK_OF_YEAR                  INT,
    IS_WEEKEND                    STRING,
    INSERT_DATETIME               TIMESTAMP NOT NULL,
    UPDATE_DATETIME                TIMESTAMP,
    SOURCE_INGESTION_LOG_ID        BIGINT
) USING DELTA
""")
# Note: attribute columns dropped NOT NULL (vs. the earlier version of this
# script) so the -1 unknown-member row below can carry a real key with every
# other column blank -- matching the reference accelerator's own
# Date_Dimension table, which is fully nullable except its key.

if spark.sql("SELECT COUNT(*) AS c FROM dimdate WHERE DATE_SK = -1").collect()[0]["c"] == 0:
    spark.sql("""
    INSERT INTO dimdate (DATE_SK, INSERT_DATETIME)
    VALUES (-1, current_timestamp())
    """)

# -----------------------------------------------------------------------------
# FACTSALESORDERDETAIL -- transactional fact, grain = one row per order line
# -----------------------------------------------------------------------------
spark.sql("""
CREATE TABLE IF NOT EXISTS factsalesorderdetail (
    SalesOrderID              INT         NOT NULL,
    SalesOrderDetailID         INT        NOT NULL,
    PRODUCT_SK                  BIGINT     NOT NULL,
    CUSTOMER_SK                  BIGINT    NOT NULL,
    SALESTERRITORY_SK             BIGINT   NOT NULL,
    DATE_SK                        INT     NOT NULL,
    OrderDate                       TIMESTAMP,
    OrderQty                         SMALLINT,
    UnitPrice                         DECIMAL(19,4),
    LineTotal                          DECIMAL(19,4),
    INSERT_DATETIME                    TIMESTAMP NOT NULL,
    UPDATE_DATETIME                    TIMESTAMP,
    SOURCE_INGESTION_LOG_ID            BIGINT
) USING DELTA
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS factsalesorderdetail_quarantine (
    SalesOrderID           INT,
    SalesOrderDetailID       INT,
    ProductID                  INT,
    CustomerID                   INT,
    TerritoryID                    INT,
    OrderDate                       TIMESTAMP,
    OrderQty                          SMALLINT,
    UnitPrice                           DECIMAL(19,4),
    LineTotal                             DECIMAL(19,4)
) USING DELTA
""")

# -----------------------------------------------------------------------------
# Verification -- lists every table just created with its row count.
# dimproduct/dimcustomer/dimsalesterritory/dimdate should each show 1 (their
# seeded -1 unknown-member row); everything else should show 0, since no
# real data has been loaded yet.
# -----------------------------------------------------------------------------
for t in [
    "dimproduct", "dimproduct_quarantine",
    "dimcustomer", "dimcustomer_quarantine",
    "dimsalesterritory", "dimsalesterritory_quarantine",
    "dimdate",
    "factsalesorderdetail", "factsalesorderdetail_quarantine",
]:
    cnt = spark.sql(f"SELECT COUNT(*) AS cnt FROM {t}").collect()[0]["cnt"]
    print(f"{t}: created, {cnt} rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
