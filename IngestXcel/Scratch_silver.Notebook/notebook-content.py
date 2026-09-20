# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0d793c52-9170-4eb5-b0b4-6bdf137c4403",
# META       "default_lakehouse_name": "Silver_LH",
# META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
# META       "known_lakehouses": [
# META         {
# META           "id": "0d793c52-9170-4eb5-b0b4-6bdf137c4403"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC  
# MAGIC CREATE TABLE fabrictraining_ingestxcel.ingestxcel_plan (
# MAGIC     Task_ID STRING,
# MAGIC     Phase STRING,
# MAGIC     Task_Name STRING,
# MAGIC     Description STRING,
# MAGIC     Depends_On STRING,
# MAGIC     Orig_Est DOUBLE,
# MAGIC     Status STRING,
# MAGIC     Revised_Est DOUBLE,
# MAGIC     Priority STRING,
# MAGIC     Notes STRING,
# MAGIC     _source_file_name STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC drop table fabrictraining_ingestxcel.ingestxcel_plan 

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM fabrictraining_ingestxcel.business_financial_data;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE SCHEMA IF NOT EXISTS fabrictraining_ingestxcel;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.product (
# MAGIC     ProductID INT, Name STRING, ProductNumber STRING, MakeFlag STRING,
# MAGIC     FinishedGoodsFlag STRING, Color STRING, SafetyStockLevel SMALLINT,
# MAGIC     ReorderPoint SMALLINT, StandardCost DECIMAL(19,4), ListPrice DECIMAL(19,4),
# MAGIC     Size STRING, SizeUnitMeasureCode STRING, WeightUnitMeasureCode STRING,
# MAGIC     Weight DECIMAL(8,2), DaysToManufacture INT, ProductLine STRING, Class STRING,
# MAGIC     Style STRING, ProductSubcategoryID INT, ProductModelID INT,
# MAGIC     SellStartDate TIMESTAMP, SellEndDate TIMESTAMP, DiscontinuedDate TIMESTAMP,
# MAGIC     rowguid STRING, ModifiedDate TIMESTAMP,
# MAGIC     SCD_END_DATE TIMESTAMP, IS_CURRENT STRING
# MAGIC ) USING DELTA;
# MAGIC -- SCD2: reuses ModifiedDate as the SCD2 start-date column, only end-date +
# MAGIC -- current-flag are new.
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.productcategory (
# MAGIC     ProductCategoryID INT, Name STRING, rowguid STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC -- SCD1, no extra columns.
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.currency (
# MAGIC     CurrencyCode STRING, Name STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC -- SCD1, no extra columns.
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.persondetails (
# MAGIC     ID INT, Name STRING, Age INT, DateOfBirth DATE, Address STRING,
# MAGIC     SCD_START_DATE TIMESTAMP, SCD_END_DATE TIMESTAMP, IS_CURRENT STRING
# MAGIC ) USING DELTA;
# MAGIC -- SCD2, no-watermark full-rescan — needs its own synthetic start-date column
# MAGIC -- since there's no real source timestamp to reuse (unlike Product).

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC select 'currency' as tablename, count(*) from fabrictraining_ingestxcel.currency --105 
# MAGIC union 
# MAGIC select 'persondetails' as tablename,count(*) from fabrictraining_ingestxcel.persondetails -- 50
# MAGIC union
# MAGIC select 'product' as tablename, count(*) from fabrictraining_ingestxcel.product -- 504
# MAGIC union
# MAGIC select 'productcategory' as tablename, count(*) from fabrictraining_ingestxcel.productcategory --4

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC  
# MAGIC select * from fabrictraining_ingestxcel.persondetails    WHERE ID = 1;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT 'product' as tablename, COUNT(*) FROM fabrictraining_ingestxcel.product
# MAGIC UNION ALL 
# MAGIC SELECT 'productcategory', COUNT(*) FROM fabrictraining_ingestxcel.productcategory
# MAGIC UNION ALL 
# MAGIC SELECT 'currency', COUNT(*) FROM fabrictraining_ingestxcel.currency
# MAGIC UNION ALL 
# MAGIC SELECT 'persondetails' as tablename, COUNT(*) FROM fabrictraining_ingestxcel.persondetails;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SHOW TABLES IN Silver_LH.default")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC 
# MAGIC SELECT * FROM fabrictraining_ingestxcel.persondetails_quarantine;
# MAGIC 
# MAGIC SELECT * from fabrictraining_ingestxcel.persondetails;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE fabrictraining_ingestxcel.persondetails_quarantine (
# MAGIC     ID INT, Name STRING, Age INT, DateOfBirth DATE, Address STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE fabrictraining_ingestxcel.product_quarantine (
# MAGIC     ProductID INT, Name STRING, ProductNumber STRING, MakeFlag STRING,
# MAGIC     FinishedGoodsFlag STRING, Color STRING, SafetyStockLevel SMALLINT,
# MAGIC     ReorderPoint SMALLINT, StandardCost DECIMAL(19,4), ListPrice DECIMAL(19,4),
# MAGIC     Size STRING, SizeUnitMeasureCode STRING, WeightUnitMeasureCode STRING,
# MAGIC     Weight DECIMAL(8,2), DaysToManufacture INT, ProductLine STRING, Class STRING,
# MAGIC     Style STRING, ProductSubcategoryID INT, ProductModelID INT,
# MAGIC     SellStartDate TIMESTAMP, SellEndDate TIMESTAMP, DiscontinuedDate TIMESTAMP,
# MAGIC     rowguid STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.productcategory_quarantine (
# MAGIC     ProductCategoryID INT, Name STRING, rowguid STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.currency_quarantine (
# MAGIC     CurrencyCode STRING, Name STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count(*) as cnt from fabrictraining_ingestxcel.persondetails;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT count(*) as cnt from fabrictraining_ingestxcel.persondetails_qurantine;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE SCHEMA IF NOT EXISTS ingestxcel_files;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE ingestxcel_files.business_financial_data (
# MAGIC     Series_reference STRING, Period STRING, Data_value STRING, Suppressed STRING,
# MAGIC     STATUS STRING, UNITS STRING, Magnitude STRING, Subject STRING, `Group` STRING,
# MAGIC     Series_title_1 STRING, Series_title_2 STRING, Series_title_3 STRING,
# MAGIC     Series_title_4 STRING, Series_title_5 STRING, _source_file_name STRING
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE ingestxcel_files.business_financial_data_quarantine (
# MAGIC     Series_reference STRING, Period STRING, Data_value STRING, Suppressed STRING,
# MAGIC     STATUS STRING, UNITS STRING, Magnitude STRING, Subject STRING, `Group` STRING,
# MAGIC     Series_title_1 STRING, Series_title_2 STRING, Series_title_3 STRING,
# MAGIC     Series_title_4 STRING, Series_title_5 STRING, _source_file_name STRING
# MAGIC ) USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) as cnt from ingestxcel_files.business_financial_data

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.person_businessentity (
    BusinessEntityID INT,
    rowguid STRING,
    ModifiedDate TIMESTAMP
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.person_businessentity_quarantine (
    BusinessEntityID INT,
    rowguid STRING,
    ModifiedDate TIMESTAMP
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT META_ORCHESTRATION_ID, TRIGGER_NAME, SOURCE_ENTITY_NAME, TARGET_ENTITY
# MAGIC FROM META_ORCHESTRATION
# MAGIC WHERE SOURCE_ENTITY_NAME = 'person_businessentity' AND TRIGGER_NAME = 'TRG_FABRICTRAINING_INGESTXCEL_SILVER_LOAD_DAILY';

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Scratch_silver, attached to Silver_LH
spark.sql("TRUNCATE TABLE fabrictraining_ingestxcel.person_businessentity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW SCHEMAS"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("SHOW TABLES IN <schema_name>"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC /* ============================================================
# MAGIC    Gold Demo — Silver onboarding metadata
# MAGIC    Tables: sales_customer, sales_salesterritory,
# MAGIC            sales_salesorderheader, sales_salesorderdetail
# MAGIC    SCD_TYPE = SCD1, PROCESSING_METHOD = FULL, no WATERMARK_COLUMN
# MAGIC    (mirrors business_financial_data's proven no-watermark pattern).
# MAGIC 
# MAGIC    Run against the IngestXcel metadata SQL database, AFTER Bronze
# MAGIC    is confirmed successful for all four entities.
# MAGIC 
# MAGIC    Silver's "source" endpoint is Bronze's own target Lakehouse
# MAGIC    endpoint (confirmed = 7, TGT_INGESTXCEL_BRONZE_LH, from your
# MAGIC    Bronze run's Get Bronze Batch output) — resolved dynamically
# MAGIC    below for robustness rather than hardcoded. Silver's own
# MAGIC    trigger name and target endpoint are derived from the existing
# MAGIC    'product' Silver entity (Production.Product's proven Silver
# MAGIC    row), the same reuse pattern used for Bronze.
# MAGIC    ============================================================ */
# MAGIC 
# MAGIC DECLARE @BronzeTargetEndpointId INT;
# MAGIC DECLARE @SilverTargetEndpointId INT;
# MAGIC DECLARE @SilverTriggerName      VARCHAR(500);
# MAGIC 
# MAGIC SELECT TOP 1 @BronzeTargetEndpointId = MO.META_CONNECTION_ENDPOINT_ID
# MAGIC FROM DBO.META_ORCHESTRATION MO
# MAGIC JOIN DBO.META_SOURCE_ENTITY MSE ON MSE.SOURCE_ENTITY_ID = MO.SOURCE_ENTITY_ID
# MAGIC WHERE MSE.SOURCE_ENTITY_NAME = 'Production.Product'
# MAGIC   AND MO.TRIGGER_NAME LIKE '%BRONZE%';
# MAGIC 
# MAGIC SELECT TOP 1
# MAGIC     @SilverTargetEndpointId = MO.META_CONNECTION_ENDPOINT_ID,
# MAGIC     @SilverTriggerName      = MO.TRIGGER_NAME
# MAGIC FROM DBO.META_ORCHESTRATION MO
# MAGIC JOIN DBO.META_SOURCE_ENTITY MSE ON MSE.SOURCE_ENTITY_ID = MO.SOURCE_ENTITY_ID
# MAGIC WHERE MSE.SOURCE_ENTITY_NAME = 'product'
# MAGIC   AND MO.TRIGGER_NAME LIKE '%SILVER%';
# MAGIC 
# MAGIC IF @BronzeTargetEndpointId IS NULL OR @SilverTargetEndpointId IS NULL OR @SilverTriggerName IS NULL
# MAGIC BEGIN
# MAGIC     RAISERROR('Could not resolve Bronze target endpoint / Silver target endpoint / Silver trigger name from Production.Product / product. Verify those rows exist as expected before running this script.', 16, 1);
# MAGIC     RETURN;
# MAGIC END
# MAGIC 
# MAGIC PRINT 'Resolved BronzeTargetEndpointId = ' + CAST(@BronzeTargetEndpointId AS VARCHAR(20));
# MAGIC PRINT 'Resolved SilverTargetEndpointId = ' + CAST(@SilverTargetEndpointId AS VARCHAR(20));
# MAGIC PRINT 'Resolved SilverTriggerName = ' + @SilverTriggerName;
# MAGIC 
# MAGIC /* ------------------------------------------------------------
# MAGIC    1. META_SOURCE_ENTITY — Silver's own row per table, pointing
# MAGIC       at Bronze's target Lakehouse endpoint as its "source"
# MAGIC       (ADR-0006/0007 standing principle: layer N reads layer
# MAGIC       N-1's own output, not the original external system).
# MAGIC    ------------------------------------------------------------ */
# MAGIC INSERT INTO DBO.META_SOURCE_ENTITY
# MAGIC     (SOURCE_ENTITY_NAME, SOURCE_CONNECTION_ENDPOINT_ID, DESCRIPTION, CREATED_BY)
# MAGIC SELECT T.NM, @BronzeTargetEndpointId, 'Gold demo — Silver entity, reads Bronze output', 'GOLD_DEMO_ONBOARDING'
# MAGIC FROM (VALUES
# MAGIC     ('sales_customer'),
# MAGIC     ('sales_salesterritory'),
# MAGIC     ('sales_salesorderheader'),
# MAGIC     ('sales_salesorderdetail')
# MAGIC ) AS T(NM)
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM DBO.META_SOURCE_ENTITY MSE WHERE MSE.SOURCE_ENTITY_NAME = T.NM
# MAGIC );
# MAGIC 
# MAGIC /* ------------------------------------------------------------
# MAGIC    2. META_ORCHESTRATION — Silver trigger row per table.
# MAGIC    ------------------------------------------------------------ */
# MAGIC INSERT INTO DBO.META_ORCHESTRATION
# MAGIC     (TRIGGER_NAME, SOURCE_ENTITY_ID, SOURCE_ENTITY_NAME, META_CONNECTION_ENDPOINT_ID,
# MAGIC      TARGET_ENTITY, PRIMARY_KEYS, PROCESSING_METHOD, CREATED_BY)
# MAGIC SELECT
# MAGIC     @SilverTriggerName,
# MAGIC     MSE.SOURCE_ENTITY_ID,
# MAGIC     MSE.SOURCE_ENTITY_NAME,
# MAGIC     @SilverTargetEndpointId,
# MAGIC     T.TARGET_ENTITY,
# MAGIC     T.PRIMARY_KEYS,
# MAGIC     'FULL',
# MAGIC     'GOLD_DEMO_ONBOARDING'
# MAGIC FROM (VALUES
# MAGIC     ('sales_customer',         'sales_customer',         'CustomerID'),
# MAGIC     ('sales_salesterritory',   'sales_salesterritory',   'TerritoryID'),
# MAGIC     ('sales_salesorderheader', 'sales_salesorderheader', 'SalesOrderID'),
# MAGIC     ('sales_salesorderdetail', 'sales_salesorderdetail', 'SalesOrderID,SalesOrderDetailID')
# MAGIC ) AS T(SOURCE_NAME, TARGET_ENTITY, PRIMARY_KEYS)
# MAGIC JOIN DBO.META_SOURCE_ENTITY MSE ON MSE.SOURCE_ENTITY_NAME = T.SOURCE_NAME
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM DBO.META_ORCHESTRATION MO
# MAGIC     WHERE MO.SOURCE_ENTITY_ID = MSE.SOURCE_ENTITY_ID AND MO.TRIGGER_NAME = @SilverTriggerName
# MAGIC );
# MAGIC 
# MAGIC /* ------------------------------------------------------------
# MAGIC    3. META_CONFIGURATION_CORE — SCD_TYPE + QUARANTINE_TABLE_NAME
# MAGIC       per entity. No WATERMARK_COLUMN row (FULL rescan every run,
# MAGIC       same proven no-watermark pattern as business_financial_data).
# MAGIC    ------------------------------------------------------------ */
# MAGIC INSERT INTO DBO.META_CONFIGURATION_CORE
# MAGIC     (SOURCE_ENTITY_ID, SOURCE_ENTITY_NAME, CONFIGURATION_CATEGORY, CONFIGURATION_NAME, CONFIGURATION_VALUE, CREATED_BY)
# MAGIC SELECT MSE.SOURCE_ENTITY_ID, MSE.SOURCE_ENTITY_NAME, 'SILVER', T.CFG_NAME, T.CFG_VALUE, 'GOLD_DEMO_ONBOARDING'
# MAGIC FROM (VALUES
# MAGIC     ('sales_customer',         'SCD_TYPE', 'SCD1'),
# MAGIC     ('sales_customer',         'QUARANTINE_TABLE_NAME', 'sales_customer_quarantine'),
# MAGIC     ('sales_salesterritory',   'SCD_TYPE', 'SCD1'),
# MAGIC     ('sales_salesterritory',   'QUARANTINE_TABLE_NAME', 'sales_salesterritory_quarantine'),
# MAGIC     ('sales_salesorderheader', 'SCD_TYPE', 'SCD1'),
# MAGIC     ('sales_salesorderheader', 'QUARANTINE_TABLE_NAME', 'sales_salesorderheader_quarantine'),
# MAGIC     ('sales_salesorderdetail', 'SCD_TYPE', 'SCD1'),
# MAGIC     ('sales_salesorderdetail', 'QUARANTINE_TABLE_NAME', 'sales_salesorderdetail_quarantine')
# MAGIC ) AS T(SOURCE_NAME, CFG_NAME, CFG_VALUE)
# MAGIC JOIN DBO.META_SOURCE_ENTITY MSE ON MSE.SOURCE_ENTITY_NAME = T.SOURCE_NAME
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1 FROM DBO.META_CONFIGURATION_CORE MCC
# MAGIC     WHERE MCC.SOURCE_ENTITY_ID = MSE.SOURCE_ENTITY_ID
# MAGIC       AND MCC.CONFIGURATION_CATEGORY = 'SILVER'
# MAGIC       AND MCC.CONFIGURATION_NAME = T.CFG_NAME
# MAGIC );
# MAGIC 
# MAGIC /* ------------------------------------------------------------
# MAGIC    Verification
# MAGIC    ------------------------------------------------------------ */
# MAGIC SELECT MSE.SOURCE_ENTITY_NAME, MO.TRIGGER_NAME, MO.TARGET_ENTITY, MO.PRIMARY_KEYS,
# MAGIC        MO.PROCESSING_METHOD, MO.META_CONNECTION_ENDPOINT_ID
# MAGIC FROM DBO.META_ORCHESTRATION MO
# MAGIC JOIN DBO.META_SOURCE_ENTITY MSE ON MSE.SOURCE_ENTITY_ID = MO.SOURCE_ENTITY_ID
# MAGIC WHERE MSE.SOURCE_ENTITY_NAME IN
# MAGIC     ('sales_customer','sales_salesterritory','sales_salesorderheader','sales_salesorderdetail')
# MAGIC   AND MO.TRIGGER_NAME = @SilverTriggerName
# MAGIC ORDER BY MSE.SOURCE_ENTITY_NAME;
# MAGIC 
# MAGIC SELECT MSE.SOURCE_ENTITY_NAME, MCC.CONFIGURATION_NAME, MCC.CONFIGURATION_VALUE
# MAGIC FROM DBO.META_CONFIGURATION_CORE MCC
# MAGIC JOIN DBO.META_SOURCE_ENTITY MSE ON MSE.SOURCE_ENTITY_ID = MCC.SOURCE_ENTITY_ID
# MAGIC WHERE MSE.SOURCE_ENTITY_NAME IN
# MAGIC     ('sales_customer','sales_salesterritory','sales_salesorderheader','sales_salesorderdetail')
# MAGIC   AND MCC.CONFIGURATION_CATEGORY = 'SILVER'
# MAGIC ORDER BY MSE.SOURCE_ENTITY_NAME, MCC.CONFIGURATION_NAME;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("DESCRIBE TABLE fabrictraining_ingestxcel.productcategory"))
display(spark.sql("DESCRIBE TABLE fabrictraining_ingestxcel.productcategory_quarantine"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================================
# Gold Demo — Silver table + quarantine skeletons
# Tables: sales_customer, sales_salesterritory,
#         sales_salesorderheader, sales_salesorderdetail
#
# Run this as a cell in a Fabric notebook with Silver_LH set as
# the DEFAULT LAKEHOUSE.
#
# Schema confirmed as fabrictraining_ingestxcel (same schema name
# reused from Bronze_LH). Column shapes match the Bronze DDL
# exactly — confirmed via productcategory (SCD1): a Silver SCD1
# table carries no extra bookkeeping columns beyond its source,
# and its quarantine table is an identical-shape copy, not a
# separate structure with added audit columns.
# ============================================================

silver_schema = "fabrictraining_ingestxcel"

table_defs = {
    "sales_customer": """
        CustomerID      INT,
        PersonID        INT,
        StoreID         INT,
        TerritoryID     INT,
        AccountNumber   STRING,
        rowguid         STRING,
        ModifiedDate    TIMESTAMP
    """,
    "sales_salesterritory": """
        TerritoryID           INT,
        Name                  STRING,
        CountryRegionCode     STRING,
        SalesTerritoryGroup   STRING,
        SalesYTD              DECIMAL(19,4),
        SalesLastYear         DECIMAL(19,4),
        CostYTD               DECIMAL(19,4),
        CostLastYear          DECIMAL(19,4),
        rowguid               STRING,
        ModifiedDate          TIMESTAMP
    """,
    "sales_salesorderheader": """
        SalesOrderID            INT,
        RevisionNumber          SMALLINT,
        OrderDate               TIMESTAMP,
        DueDate                 TIMESTAMP,
        ShipDate                TIMESTAMP,
        Status                  SMALLINT,
        OnlineOrderFlag         BOOLEAN,
        SalesOrderNumber        STRING,
        PurchaseOrderNumber     STRING,
        AccountNumber           STRING,
        CustomerID              INT,
        SalesPersonID           INT,
        TerritoryID             INT,
        BillToAddressID         INT,
        ShipToAddressID         INT,
        ShipMethodID            INT,
        CreditCardID            INT,
        CreditCardApprovalCode  STRING,
        CurrencyRateID          INT,
        SubTotal                DECIMAL(19,4),
        TaxAmt                  DECIMAL(19,4),
        Freight                 DECIMAL(19,4),
        TotalDue                DECIMAL(19,4),
        Comment                 STRING,
        rowguid                 STRING,
        ModifiedDate            TIMESTAMP
    """,
    "sales_salesorderdetail": """
        SalesOrderID            INT,
        SalesOrderDetailID      INT,
        CarrierTrackingNumber   STRING,
        OrderQty                SMALLINT,
        ProductID               INT,
        SpecialOfferID          INT,
        UnitPrice               DECIMAL(19,4),
        UnitPriceDiscount       DECIMAL(19,4),
        LineTotal                DECIMAL(19,4),
        rowguid                 STRING,
        ModifiedDate            TIMESTAMP
    """,
}

for table_name, cols in table_defs.items():
    spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_schema}.{table_name} ({cols}) USING DELTA")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {silver_schema}.{table_name}_quarantine ({cols}) USING DELTA")

print("Silver + quarantine skeleton tables created (or already existed) under schema:", silver_schema)
for table_name in table_defs:
    print(f"--- {table_name} ---")
    display(spark.sql(f"DESCRIBE TABLE {silver_schema}.{table_name}"))
    print(f"--- {table_name}_quarantine ---")
    display(spark.sql(f"DESCRIBE TABLE {silver_schema}.{table_name}_quarantine"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
