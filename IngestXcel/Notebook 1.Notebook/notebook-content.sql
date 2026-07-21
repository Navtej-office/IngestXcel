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
