# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a6ee5274-a432-4395-b3a2-4f18886fce18",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
# META       "known_lakehouses": [
# META         {
# META           "id": "a6ee5274-a432-4395-b3a2-4f18886fce18"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS fabrictraining_ingestxcel.ingestxcel_plan;
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
# MAGIC 
# MAGIC 


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT COUNT(*) FROM fabrictraining_ingestxcel.ingestxcel_plan;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


notebookutils.fs.ls("abfss://IngestXcel@onelake.dfs.fabric.microsoft.com/Bronze_LH.Lakehouse/Files/TestExcelSource/IngestXcel_Plan")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC  
# MAGIC  delete FROM Bronze_LH.fabrictraining_ingestxcel.ingestxcel_plan  
# MAGIC  

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC  SELECT  * FROM Bronze_LH.fabrictraining_ingestxcel.business_financial_data  
# MAGIC  

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_LH.fabrictraining_ingestxcel.business_financial_data LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("=== Catalog's view of the schema ===")
spark.sql("DESCRIBE TABLE fabrictraining_ingestxcel.business_financial_data").show(100, truncate=False)

print("=== Actual Delta log schema (bypasses the catalog entirely) ===")
spark.read.format("delta").load("abfss://2773bec8-6438-4872-b2ee-d34f1a32b3a9@onelake.dfs.fabric.microsoft.com/7e491b48-5978-4a73-bbe7-b98df9812e65/Tables/fabrictraining_ingestxcel/business_financial_data").printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC DROP TABLE IF EXISTS fabrictraining_ingestxcel.business_financial_data;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.business_financial_data (
# MAGIC     Series_reference STRING,
# MAGIC     Period STRING,
# MAGIC     Data_value STRING,
# MAGIC     Suppressed STRING,
# MAGIC     STATUS STRING,
# MAGIC     UNITS STRING,
# MAGIC     Magnitude STRING,
# MAGIC     Subject STRING,
# MAGIC     `Group` STRING,
# MAGIC     Series_title_1 STRING,
# MAGIC     Series_title_2 STRING,
# MAGIC     Series_title_3 STRING,
# MAGIC     Series_title_4 STRING,
# MAGIC     Series_title_5 STRING,
# MAGIC     _source_file_name STRING
# MAGIC ) USING DELTA;

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

import fnmatch
all_files = notebookutils.fs.ls("abfss://2773bec8-6438-4872-b2ee-d34f1a32b3a9@onelake.dfs.fabric.microsoft.com/7e491b48-5978-4a73-bbe7-b98df9812e65/Files/TestCSV")
for f in all_files:
    print(f.name, f.modifyTime, fnmatch.fnmatch(f.name, "business-financial-data-*.csv"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schemas = [row["namespace"] for row in spark.sql("SHOW SCHEMAS IN Bronze_LH").collect()]
for schema in schemas:
    print(f"Schema: {schema}")
    try:
        tables = [row["tableName"] for row in spark.sql(f"SHOW TABLES IN Bronze_LH.`{schema}`").collect()]
        for t in tables:
            print(f"   - {schema}.{t}")
    except Exception as e:
        print(f"   could not list tables: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Run against Bronze_LH, then reattach and run against Silver_LH
print(spark.sql("SHOW SCHEMAS IN Bronze_LH").collect())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.product (
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
# MAGIC CREATE TABLE fabrictraining_ingestxcel.productcategory (
# MAGIC     ProductCategoryID INT, Name STRING, rowguid STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.currency (
# MAGIC     CurrencyCode STRING, Name STRING, ModifiedDate TIMESTAMP
# MAGIC ) USING DELTA;
# MAGIC 
# MAGIC CREATE TABLE fabrictraining_ingestxcel.persondetails (
# MAGIC     ID INT, Name STRING, Age INT, DateOfBirth DATE, Address STRING
# MAGIC ) USING DELTA;

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

# MAGIC %%sql
# MAGIC 
# MAGIC select * from fabrictraining_ingestxcel.currency where    CurrencyCode ='AED';--105 


# METADATA ********************

# META {
# META   "language": "sparksql",
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
# MAGIC  
# MAGIC select * from fabrictraining_ingestxcel.product WHERE ProductID = 1;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC  
# MAGIC select * from fabrictraining_ingestxcel.productcategory WHERE ProductCategoryID = 1;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC select count(*) as cnt  from fabrictraining_ingestxcel.persondetails;


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

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT O.META_ORCHESTRATION_ID, O.TRIGGER_NAME, O.PRIMARY_KEYS, O.PROCESSING_METHOD, O.TARGET_ENTITY
# MAGIC FROM META_ORCHESTRATION O
# MAGIC INNER JOIN META_SOURCE_ENTITY SE ON SE.SOURCE_ENTITY_ID = O.SOURCE_ENTITY_ID
# MAGIC WHERE SE.SOURCE_ENTITY_NAME = 'Person.BusinessEntity';

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

# Scratch_Bronze, attached to Bronze_LH
spark.sql("TRUNCATE TABLE fabrictraining_ingestxcel.person_businessentity")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
