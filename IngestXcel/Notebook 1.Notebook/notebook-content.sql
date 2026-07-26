-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "synapse_pyspark"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "7e491b48-5978-4a73-bbe7-b98df9812e65",
-- META       "default_lakehouse_name": "Bronze_LH",
-- META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "7e491b48-5978-4a73-bbe7-b98df9812e65"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

-- Welcome to your new notebook
-- Type here in the cell editor to add code!


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%sql
-- MAGIC CREATE TABLE IF NOT EXISTS FABRICTRAINING_INGESTXCEL.PERSON_BUSINESSENTITYCONTACT (
-- MAGIC     BusinessEntityID INT NOT NULL,
-- MAGIC     PersonID          INT NOT NULL,
-- MAGIC     ContactTypeID     INT NOT NULL,
-- MAGIC     rowguid           STRING NOT NULL,
-- MAGIC     ModifiedDate      TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************

-- # ============================================================
-- # Lowercase migration for FABRICTRAINING_INGESTXCEL Bronze schema
-- # Run this in a Fabric notebook with Bronze_LH attached as the
-- # default Lakehouse. Drops the existing (likely mis-cased) schema/
-- # table and recreates everything in lowercase, with case
-- # sensitivity forced so Spark doesn't silently re-normalize anything.
-- # ============================================================
-- 
-- spark.conf.set("spark.sql.caseSensitive", True)
-- 
-- # --- Check current state first (uncomment to inspect before dropping)
-- # spark.sql("SHOW SCHEMAS").show(truncate=False)
-- # spark.sql("SHOW TABLES IN FABRICTRAINING_INGESTXCEL").show(truncate=False)
-- 
-- # --- Drop old objects — covers both possible existing casings
-- spark.sql("DROP TABLE IF EXISTS FABRICTRAINING_INGESTXCEL.PERSON_BUSINESSENTITYCONTACT")
-- spark.sql("DROP TABLE IF EXISTS fabrictraining_ingestxcel.person_businessentitycontact")
-- spark.sql("DROP SCHEMA IF EXISTS FABRICTRAINING_INGESTXCEL")
-- spark.sql("DROP SCHEMA IF EXISTS fabrictraining_ingestxcel")
-- 
-- # --- Recreate schema, lowercase
-- spark.sql("CREATE SCHEMA IF NOT EXISTS fabrictraining_ingestxcel")
-- 
-- # --- Recreate all 4 Fabric SQL DB Bronze tables, lowercase,
-- #     matching each source table's actual AdventureWorks structure
-- 
-- spark.sql("""
-- CREATE TABLE fabrictraining_ingestxcel.person_businessentity (
--     BusinessEntityID INT NOT NULL,
--     rowguid          STRING NOT NULL,
--     ModifiedDate     TIMESTAMP NOT NULL
-- ) USING DELTA
-- """)
-- 
-- spark.sql("""
-- CREATE TABLE fabrictraining_ingestxcel.person_businessentitycontact (
--     BusinessEntityID INT NOT NULL,
--     PersonID         INT NOT NULL,
--     ContactTypeID    INT NOT NULL,
--     rowguid          STRING NOT NULL,
--     ModifiedDate     TIMESTAMP NOT NULL
-- ) USING DELTA
-- """)
-- 
-- spark.sql("""
-- CREATE TABLE fabrictraining_ingestxcel.humanresources_employeepayhistory (
--     BusinessEntityID INT NOT NULL,
--     RateChangeDate   TIMESTAMP NOT NULL,
--     Rate             DECIMAL(19,4) NOT NULL,
--     PayFrequency     TINYINT NOT NULL,
--     ModifiedDate     TIMESTAMP NOT NULL
-- ) USING DELTA
-- """)
-- 
-- spark.sql("""
-- CREATE TABLE fabrictraining_ingestxcel.humanresources_employeedepartmenthistory (
--     BusinessEntityID INT NOT NULL,
--     DepartmentID     SMALLINT NOT NULL,
--     ShiftID          TINYINT NOT NULL,
--     StartDate        DATE NOT NULL,
--     EndDate          DATE,
--     ModifiedDate     TIMESTAMP NOT NULL
-- ) USING DELTA
-- """)
-- 
-- # --- Verify
-- spark.sql("SHOW TABLES IN fabrictraining_ingestxcel").show(truncate=False)


-- CELL ********************

spark.conf.set("spark.sql.caseSensitive", True)
spark.sql("DROP TABLE IF EXISTS FABRICTRAINING_INGESTXCEL.PERSON_BUSINESSENTITYCONTACT")
spark.sql("DROP TABLE IF EXISTS fabrictraining_ingestxcel.person_businessentitycontact")
spark.sql("DROP SCHEMA IF EXISTS FABRICTRAINING_INGESTXCEL")
spark.sql("DROP SCHEMA IF EXISTS fabrictraining_ingestxcel")
 
# --- Recreate schema, lowercase
spark.sql("CREATE SCHEMA IF NOT EXISTS fabrictraining_ingestxcel")
 
 spark.conf.set("spark.sql.caseSensitive", True)
spark.sql("""
CREATE TABLE fabrictraining_ingestxcel.person_businessentity (
    BusinessEntityID INT NOT NULL,
    rowguid          STRING NOT NULL,
    ModifiedDate     TIMESTAMP NOT NULL
) USING DELTA
""")
 
spark.sql("""
CREATE TABLE fabrictraining_ingestxcel.person_businessentitycontact (
    BusinessEntityID INT NOT NULL,
    PersonID         INT NOT NULL,
    ContactTypeID    INT NOT NULL,
    rowguid          STRING NOT NULL,
    ModifiedDate     TIMESTAMP NOT NULL
) USING DELTA
""")
 
spark.sql("""
CREATE TABLE fabrictraining_ingestxcel.humanresources_employeepayhistory (
    BusinessEntityID INT NOT NULL,
    RateChangeDate   TIMESTAMP NOT NULL,
    Rate             DECIMAL(19,4) NOT NULL,
    PayFrequency     TINYINT NOT NULL,
    ModifiedDate     TIMESTAMP NOT NULL
) USING DELTA
""")
 
spark.sql("""
CREATE TABLE fabrictraining_ingestxcel.humanresources_employeedepartmenthistory (
    BusinessEntityID INT NOT NULL,
    DepartmentID     SMALLINT NOT NULL,
    ShiftID          TINYINT NOT NULL,
    StartDate        DATE NOT NULL,
    EndDate          DATE,
    ModifiedDate     TIMESTAMP NOT NULL
) USING DELTA
""")

spark.sql("SHOW TABLES IN fabrictraining_ingestxcel").show(truncate=False)

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%spark
-- MAGIC spark.sql("CREATE SCHEMA IF NOT EXISTS fabrictraining_ingestxcel")
-- MAGIC  

-- METADATA ********************

-- META {
-- META   "language": "scala",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.conf.set("spark.sql.caseSensitive", True)
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE fabrictraining_ingestxcel.person_businessentity (
-- MAGIC     BusinessEntityID INT NOT NULL,
-- MAGIC     rowguid          STRING NOT NULL,
-- MAGIC     ModifiedDate     TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC  
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE fabrictraining_ingestxcel.person_businessentitycontact (
-- MAGIC     BusinessEntityID INT NOT NULL,
-- MAGIC     PersonID         INT NOT NULL,
-- MAGIC     ContactTypeID    INT NOT NULL,
-- MAGIC     rowguid          STRING NOT NULL,
-- MAGIC     ModifiedDate     TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC  
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE fabrictraining_ingestxcel.humanresources_employeepayhistory (
-- MAGIC     BusinessEntityID INT NOT NULL,
-- MAGIC     RateChangeDate   TIMESTAMP NOT NULL,
-- MAGIC     Rate             DECIMAL(19,4) NOT NULL,
-- MAGIC     PayFrequency     TINYINT NOT NULL,
-- MAGIC     ModifiedDate     TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC  
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE fabrictraining_ingestxcel.humanresources_employeedepartmenthistory (
-- MAGIC     BusinessEntityID INT NOT NULL,
-- MAGIC     DepartmentID     SMALLINT NOT NULL,
-- MAGIC     ShiftID          TINYINT NOT NULL,
-- MAGIC     StartDate        DATE NOT NULL,
-- MAGIC     EndDate          DATE,
-- MAGIC     ModifiedDate     TIMESTAMP NOT NULL
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC 
-- MAGIC spark.sql("SHOW TABLES IN fabrictraining_ingestxcel").show(truncate=False)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.sql("SELECT BusinessEntityID, ModifiedDate FROM fabrictraining_ingestxcel.person_businessentity WHERE BusinessEntityID IN (1,2) ORDER BY BusinessEntityID, ModifiedDate").show()

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # SCRATCH CELL - throwaway, not part of the notebook's 6 real cells.
-- MAGIC # Creates a tiny synthetic staged-Parquet file so we can test Cells 3-6
-- MAGIC # without a real pipeline run.
-- MAGIC test_data = [
-- MAGIC     (1, "1970 Napa Ct", "None", "Bothell", 1, "98011", "POINT (-122.207 47.760)", "aaaaaaaa-0000-0000-0000-000000000001", "2026-06-01 10:00:00"),
-- MAGIC     (2, "9833 Mt. Dias Blv.", "None", "Bothell", 1, "98011", "POINT (-122.208 47.761)", "aaaaaaaa-0000-0000-0000-000000000002", "2026-07-01 09:30:00"),
-- MAGIC ]
-- MAGIC test_columns = ["AddressID", "AddressLine1", "AddressLine2", "City", "StateProvinceID", "PostalCode", "SpatialLocation", "rowguid", "ModifiedDate"]
-- MAGIC test_df = spark.createDataFrame(test_data, test_columns)
-- MAGIC 
-- MAGIC test_staging_path = f"abfss://{'2773bec8-6438-4872-b2ee-d34f1a32b3a9'}@onelake.dfs.fabric.microsoft.com/{'7e491b48-5978-4a73-bbe7-b98df9812e65'}/Files/test_staging/person_address_test/"
-- MAGIC test_df.write.mode("overwrite").parquet(test_staging_path)
-- MAGIC print(test_staging_path)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC # SCRATCH CELL - injects one synthetic null-PK row directly into Bronze,
-- MAGIC # purely to test Silver's quarantine path. Not part of the real pipeline.
-- MAGIC import uuid
-- MAGIC from datetime import datetime
-- MAGIC from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
-- MAGIC 
-- MAGIC schema = StructType([
-- MAGIC     StructField("BusinessEntityID", IntegerType(), True),
-- MAGIC     StructField("rowguid", StringType(), True),
-- MAGIC     StructField("ModifiedDate", TimestampType(), True),
-- MAGIC ])
-- MAGIC 
-- MAGIC test_data = [(None, str(uuid.uuid4()), datetime(2026, 7, 24, 0, 0, 0))]
-- MAGIC test_df = spark.createDataFrame(test_data, schema=schema)
-- MAGIC 
-- MAGIC bronze_path = "abfss://2773bec8-6438-4872-b2ee-d34f1a32b3a9@onelake.dfs.fabric.microsoft.com/7e491b48-5978-4a73-bbe7-b98df9812e65/Tables/fabrictraining_ingestxcel/person_businessentity"
-- MAGIC test_df.write.format("delta").mode("append").option("mergeSchema", "true").save(bronze_path)
-- MAGIC print("Injected 1 synthetic null-PK row into Bronze")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.sql("""
-- MAGIC ALTER TABLE delta.`abfss://2773bec8-6438-4872-b2ee-d34f1a32b3a9@onelake.dfs.fabric.microsoft.com/7e491b48-5978-4a73-bbe7-b98df9812e65/Tables/fabrictraining_ingestxcel/person_businessentity`
-- MAGIC ALTER COLUMN BusinessEntityID DROP NOT NULL
-- MAGIC """)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS delta.`abfss://2773bec8-6438-4872-b2ee-d34f1a32b3a9@onelake.dfs.fabric.microsoft.com/0d793c52-9170-4eb5-b0b4-6bdf137c4403/Tables/fabrictraining_ingestxcel/person_businessentity_quarantine`
-- MAGIC (BusinessEntityID INT, rowguid STRING, ModifiedDate TIMESTAMP)
-- MAGIC USING DELTA
-- MAGIC """)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC mssparkutils.session.stop()

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE SCHEMA IF NOT EXISTS fabrictraining_ingestxcel
-- MAGIC """)
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.product (
-- MAGIC     ProductID INT,
-- MAGIC     Name STRING,
-- MAGIC     ProductNumber STRING,
-- MAGIC     MakeFlag STRING,
-- MAGIC     FinishedGoodsFlag STRING,
-- MAGIC     Color STRING,
-- MAGIC     SafetyStockLevel SMALLINT,
-- MAGIC     ReorderPoint SMALLINT,
-- MAGIC     StandardCost DECIMAL(19,4),
-- MAGIC     ListPrice DECIMAL(19,4),
-- MAGIC     Size STRING,
-- MAGIC     SizeUnitMeasureCode STRING,
-- MAGIC     WeightUnitMeasureCode STRING,
-- MAGIC     Weight DECIMAL(8,2),
-- MAGIC     DaysToManufacture INT,
-- MAGIC     ProductLine STRING,
-- MAGIC     Class STRING,
-- MAGIC     Style STRING,
-- MAGIC     ProductSubcategoryID INT,
-- MAGIC     ProductModelID INT,
-- MAGIC     SellStartDate TIMESTAMP,
-- MAGIC     SellEndDate TIMESTAMP,
-- MAGIC     DiscontinuedDate TIMESTAMP,
-- MAGIC     rowguid STRING,
-- MAGIC     ModifiedDate TIMESTAMP
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.productcategory (
-- MAGIC     ProductCategoryID INT,
-- MAGIC     Name STRING,
-- MAGIC     rowguid STRING,
-- MAGIC     ModifiedDate TIMESTAMP
-- MAGIC ) USING DELTA
-- MAGIC """)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- MARKDOWN ********************


-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.sql("""
-- MAGIC CREATE SCHEMA IF NOT EXISTS fabrictraining_ingestxcel
-- MAGIC """)
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.product (
-- MAGIC     ProductID INT,
-- MAGIC     Name STRING,
-- MAGIC     ProductNumber STRING,
-- MAGIC     MakeFlag STRING,
-- MAGIC     FinishedGoodsFlag STRING,
-- MAGIC     Color STRING,
-- MAGIC     SafetyStockLevel SMALLINT,
-- MAGIC     ReorderPoint SMALLINT,
-- MAGIC     StandardCost DECIMAL(19,4),
-- MAGIC     ListPrice DECIMAL(19,4),
-- MAGIC     Size STRING,
-- MAGIC     SizeUnitMeasureCode STRING,
-- MAGIC     WeightUnitMeasureCode STRING,
-- MAGIC     Weight DECIMAL(8,2),
-- MAGIC     DaysToManufacture INT,
-- MAGIC     ProductLine STRING,
-- MAGIC     Class STRING,
-- MAGIC     Style STRING,
-- MAGIC     ProductSubcategoryID INT,
-- MAGIC     ProductModelID INT,
-- MAGIC     SellStartDate TIMESTAMP,
-- MAGIC     SellEndDate TIMESTAMP,
-- MAGIC     DiscontinuedDate TIMESTAMP,
-- MAGIC     rowguid STRING,
-- MAGIC     ModifiedDate TIMESTAMP,
-- MAGIC     SCD_END_DATE TIMESTAMP,
-- MAGIC     IS_CURRENT STRING
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.productcategory (
-- MAGIC     ProductCategoryID INT,
-- MAGIC     Name STRING,
-- MAGIC     rowguid STRING,
-- MAGIC     ModifiedDate TIMESTAMP
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC 
-- MAGIC # -----------------------------------------------------------------------
-- MAGIC # QUARANTINE tables (Silver_LH) — same shape as their Silver counterparts,
-- MAGIC # no SCD columns needed (a quarantined row was rejected before merge).
-- MAGIC # -----------------------------------------------------------------------
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.product_quarantine (
-- MAGIC     ProductID INT, Name STRING, ProductNumber STRING, MakeFlag STRING,
-- MAGIC     FinishedGoodsFlag STRING, Color STRING, SafetyStockLevel SMALLINT,
-- MAGIC     ReorderPoint SMALLINT, StandardCost DECIMAL(19,4), ListPrice DECIMAL(19,4),
-- MAGIC     Size STRING, SizeUnitMeasureCode STRING, WeightUnitMeasureCode STRING,
-- MAGIC     Weight DECIMAL(8,2), DaysToManufacture INT, ProductLine STRING,
-- MAGIC     Class STRING, Style STRING, ProductSubcategoryID INT, ProductModelID INT,
-- MAGIC     SellStartDate TIMESTAMP, SellEndDate TIMESTAMP, DiscontinuedDate TIMESTAMP,
-- MAGIC     rowguid STRING, ModifiedDate TIMESTAMP
-- MAGIC ) USING DELTA
-- MAGIC """)
-- MAGIC 
-- MAGIC spark.sql("""
-- MAGIC CREATE TABLE IF NOT EXISTS fabrictraining_ingestxcel.productcategory_quarantine (
-- MAGIC     ProductCategoryID INT, Name STRING, rowguid STRING, ModifiedDate TIMESTAMP
-- MAGIC ) USING DELTA
-- MAGIC """)

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

-- MAGIC %%pyspark
-- MAGIC spark.sql("""delete from fabrictraining_ingestxcel.product""")
-- MAGIC spark.sql("""delete from fabrictraining_ingestxcel.productcategory""")

-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

--Bronze table creation
CREATE TABLE fabrictraining_ingestxcel.persondetails (
    ID INT,
    Name STRING,
    Age INT,
    DateOfBirth DATE,
    Address STRING
) USING DELTA;

CREATE TABLE fabrictraining_ingestxcel.currency (
    CurrencyCode STRING,
    Name STRING,
    ModifiedDate TIMESTAMP
) USING DELTA;


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

 
CREATE TABLE fabrictraining_ingestxcel.persondetails (
    ID INT,
    Name STRING,
    Age INT,
    DateOfBirth DATE,
    Address STRING,
    SCD_START_DATE TIMESTAMP,
    SCD_END_DATE TIMESTAMP,
    IS_CURRENT STRING
) USING DELTA;

CREATE TABLE fabrictraining_ingestxcel.currency (
    CurrencyCode STRING,
    Name STRING,
    ModifiedDate TIMESTAMP
) USING DELTA;
 

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
