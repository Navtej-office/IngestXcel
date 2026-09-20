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

source_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"   # IngestXcel workspace
source_lakehouse_id = "0d793c52-9170-4eb5-b0b4-6bdf137c4403"   # Silver_LH
source_schema_name  = "fabrictraining_ingestxcel"
source_entity       = "sales_salesorderheader"

target_entity = "dimdate"

buffer_years_past_max = 1

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from datetime import date
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    silver_path = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Tables/{source_schema_name}/{source_entity}"
    )
    header_df = spark.read.format("delta").load(silver_path)

    date_bounds = header_df.agg(
        F.min("OrderDate").alias("min_dt"),
        F.max("OrderDate").alias("max_dt"),
    ).collect()[0]

    min_year = date_bounds["min_dt"].year
    max_year = date_bounds["max_dt"].year + buffer_years_past_max

    range_start = date(min_year, 1, 1)
    range_end = date(max_year, 12, 31)

    print(f"Silver OrderDate span: {date_bounds['min_dt']} to {date_bounds['max_dt']}")
    print(f"dimdate will cover:    {range_start} to {range_end}")
except Exception as e:
    raise Exception(f"[Determine Date Range] Failed to read '{silver_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    calendar_df = (
        spark.sql(f"""
            SELECT explode(sequence(to_date('{range_start}'), to_date('{range_end}'), interval 1 day)) AS CALENDAR_DATE
        """)
        .withColumn("DATE_SK", F.date_format(F.col("CALENDAR_DATE"), "yyyyMMdd").cast("int"))
        .withColumn("YEAR", F.year("CALENDAR_DATE"))
        .withColumn("QUARTER", F.quarter("CALENDAR_DATE"))
        .withColumn("MONTH", F.month("CALENDAR_DATE"))
        .withColumn("MONTH_NAME", F.date_format("CALENDAR_DATE", "MMMM"))
        .withColumn("DAY", F.dayofmonth("CALENDAR_DATE"))
        .withColumn("DAY_OF_WEEK", F.dayofweek("CALENDAR_DATE"))  # Spark: 1=Sunday .. 7=Saturday
        .withColumn("DAY_NAME", F.date_format("CALENDAR_DATE", "EEEE"))
        .withColumn("WEEK_OF_YEAR", F.weekofyear("CALENDAR_DATE"))
        .withColumn("IS_WEEKEND", F.when(F.col("DAY_OF_WEEK").isin(1, 7), "Y").otherwise("N"))
        .withColumn("INSERT_DATETIME", F.current_timestamp())
        .withColumn("UPDATE_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("SOURCE_INGESTION_LOG_ID", F.lit(None).cast("bigint"))
    )

    rows_generated = calendar_df.count()
    print(f"Generated {rows_generated} calendar rows")
except Exception as e:
    raise Exception(f"[Build Calendar] Failed to build date range: {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if not spark.catalog.tableExists(target_entity):
        raise Exception(
            f"Target table '{target_entity}' doesn't exist yet. Run "
            f"08_gold_table_ddl.py first -- it also seeds the -1 unknown-"
            f"member row already expected to be there."
        )

    target_table = DeltaTable.forName(spark, target_entity)
    (
        target_table.alias("current")
        .merge(calendar_df.alias("new"), "current.DATE_SK = new.DATE_SK")
        .whenNotMatchedInsertAll()
        .execute()
    )
except Exception as e:
    raise Exception(f"[Merge dimdate] Failed to merge into '{target_entity}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_count = spark.sql(f"SELECT COUNT(*) AS c FROM {target_entity}").collect()[0]["c"]
print(json.dumps({
    "status": "SUCCESS",
    "rows_generated": rows_generated,
    "range_start": str(range_start),
    "range_end": str(range_end),
    "target_table_total_rows": final_count,
}, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
