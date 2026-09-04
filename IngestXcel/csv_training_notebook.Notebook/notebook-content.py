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
# META         },
# META         {
# META           "id": "0d793c52-9170-4eb5-b0b4-6bdf137c4403"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS ingestxcel_files.book_catalog")

spark.sql("""
CREATE TABLE ingestxcel_files.book_catalog (
    BookID STRING,
    Title STRING,
    Author STRING,
    Genre STRING,
    Price STRING,
    LastUpdated STRING
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ingestxcel_files.book_catalog (
    BookID INT,
    Title STRING,
    Author STRING,
    Genre STRING,
    Price DECIMAL(10,2),
    LastUpdated TIMESTAMP,
    SCD_END_DATE TIMESTAMP,
    IS_CURRENT STRING
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
CREATE TABLE IF NOT EXISTS ingestxcel_files.book_catalog_quarantine (
    BookID INT,
    Title STRING,
    Author STRING,
    Genre STRING,
    Price DECIMAL(10,2),
    LastUpdated TIMESTAMP
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("DROP TABLE IF EXISTS ingestxcel_files.book_catalog")

spark.sql("""
CREATE TABLE ingestxcel_files.book_catalog (
    BookID STRING,
    Title STRING,
    Author STRING,
    Genre STRING,
    Price STRING,
    LastUpdated STRING
) USING DELTA
""")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
