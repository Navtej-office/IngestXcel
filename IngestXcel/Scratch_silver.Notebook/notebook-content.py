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
