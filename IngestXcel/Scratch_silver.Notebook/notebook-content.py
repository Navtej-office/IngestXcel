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
