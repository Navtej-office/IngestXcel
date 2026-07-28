# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7e491b48-5978-4a73-bbe7-b98df9812e65",
# META       "default_lakehouse_name": "Bronze_LH",
# META       "default_lakehouse_workspace_id": "2773bec8-6438-4872-b2ee-d34f1a32b3a9",
# META       "known_lakehouses": [
# META         {
# META           "id": "7e491b48-5978-4a73-bbe7-b98df9812e65"
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
