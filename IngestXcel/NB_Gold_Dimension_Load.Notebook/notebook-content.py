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

meta_orchestration_id = None

source_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"   # IngestXcel workspace
source_lakehouse_id = "0d793c52-9170-4eb5-b0b4-6bdf137c4403"   # Silver_LH
source_schema_name  = "fabrictraining_ingestxcel"
source_entity       = "sales_salesterritory"

target_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"
target_lakehouse_id = "c798f7f7-f235-4ec9-a90a-cc9489233d65"   # Gold_LH
target_entity        = "dimsalesterritory"

primary_keys          = "TerritoryID"
watermark_column       = None
watermark_value_used   = None

scd2_start_date_col    = "SCD_START_DATE"
scd2_end_date_col      = "SCD_END_DATE"
scd2_current_flag_col  = "IS_CURRENT"
surrogate_key_column   = "SALESTERRITORY_SK"
quarantine_table_name  = "dimsalesterritory_quarantine"

source_ingestion_log_id = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, DecimalType
from delta.tables import DeltaTable
from datetime import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    primary_key_list = [k.strip() for k in primary_keys.split(",")]
 
    silver_path = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Tables/{source_schema_name}/{source_entity}"
    )
 
    silver_df = spark.read.format("delta").load(silver_path)
 
    # If Silver's own source is SCD2 (only 'product' in v1 scope), keep only
    # its currently-active version and drop its SCD2 bookkeeping so it can't
    # collide with Gold's own columns of the same name.
    if "IS_CURRENT" in silver_df.columns:
        silver_df = silver_df.filter(F.col("IS_CURRENT") == "Y")
    # NOTE: ModifiedDate is deliberately NOT dropped here even though it's
    # excluded from the Gold table DDL -- dimproduct needs it downstream as
    # its watermark_column (Cell 5's version_ts, Cell 6's watermark_value_new).
    # It's dropped explicitly, right before the final write, in Cell 5 --
    # after anything that needs it has already used it.
    silver_df = silver_df.drop("rowguid", "SCD_END_DATE", "IS_CURRENT")
 
    if watermark_column:
        new_data = silver_df.filter(F.col(watermark_column) > F.lit(watermark_value_used))
    else:
        # FULL rescan (dimcustomer/dimsalesterritory) -- correctness comes from
        # the hash-compare below, not from this filter; matches Silver's own
        # no-watermark reasoning (ADR-0009) one layer down.
        new_data = silver_df
 
    rows_read = new_data.count()
except Exception as e:
    raise Exception(f"[Read Silver Source] Failed to read '{silver_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if rows_read > 0:
        null_pk_condition = " OR ".join([f"{k} IS NULL" for k in primary_key_list])
        quarantined_null_pk = new_data.filter(null_pk_condition)
        new_data_valid = new_data.filter(f"NOT ({null_pk_condition})")
 
        dedup_order = F.col(watermark_column).desc() if watermark_column else F.monotonically_increasing_id().desc()
        dedup_window = Window.partitionBy(*primary_key_list).orderBy(dedup_order)
        deduped_data = (
            new_data_valid
            .withColumn("_dedup_rank", F.row_number().over(dedup_window))
            .filter(F.col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )
        rows_quarantined = quarantined_null_pk.count()
    else:
        deduped_data = new_data
        quarantined_null_pk = new_data.limit(0)
        rows_quarantined = 0
except Exception as e:
    raise Exception(f"[Dedup Batch] Failed to dedup on [{primary_keys}]: {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    if rows_quarantined > 0:
        (
            quarantined_null_pk.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(quarantine_table_name)
        )
except Exception as e:
    raise Exception(f"[Write Quarantine] Failed to write to quarantine table: {e}") from e

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
            f"08_gold_table_ddl.py first (with Gold_LH as that notebook's "
            f"default lakehouse) -- it also seeds the required "
            f"surrogate-key=-1 unknown-member row this notebook depends on."
        )

    pk_join_condition = " AND ".join([f"current.`{k}` <=> new.`{k}`" for k in primary_key_list])
    run_ts = datetime.now()
    version_ts = F.col(watermark_column) if watermark_column else F.lit(run_ts)

    existing_active = (
        spark.table(target_entity)
        .filter(F.col(scd2_current_flag_col) == "Y")
    )

    hash_exclude_cols = (
        set(primary_key_list)
        | {watermark_column, scd2_start_date_col, scd2_end_date_col, scd2_current_flag_col,
           surrogate_key_column, "INSERT_DATETIME", "UPDATE_DATETIME", "SOURCE_INGESTION_LOG_ID",
           "ModifiedDate"}
        - {None}
    )
    new_hash_cols = sorted([c for c in deduped_data.columns if c not in hash_exclude_cols])
    existing_hash_cols = sorted([c for c in existing_active.columns if c not in hash_exclude_cols])

    def _hash_select(df, pk_cols, hash_cols):
        exprs = []
        for c in hash_cols:
            if isinstance(df.schema[c].dataType, DecimalType):
                exprs.append(F.col(c).cast(DecimalType(19, 4)).alias(c))
            else:
                exprs.append(F.col(c).alias(c))
        return df.select(*pk_cols, F.hash(*exprs).alias("row_hash"))

    new_hashed = _hash_select(deduped_data, primary_key_list, new_hash_cols).withColumnRenamed("row_hash", "new_hash")
    existing_hashed = _hash_select(existing_active, primary_key_list, existing_hash_cols).withColumnRenamed("row_hash", "existing_hash")

    compared = new_hashed.join(existing_hashed, primary_key_list, "left_outer")
    changed_keys = compared.filter(
        (F.col("existing_hash").isNotNull()) & (F.col("new_hash") != F.col("existing_hash"))
    ).select(*primary_key_list).distinct()
    new_keys = compared.filter(F.col("existing_hash").isNull()).select(*primary_key_list).distinct()

    changed_new_rows = deduped_data.join(changed_keys, primary_key_list, "inner")
    brand_new_rows = deduped_data.join(new_keys, primary_key_list, "inner")

    close_out_rows = (
        existing_active.join(changed_keys, primary_key_list, "inner").alias("e")
        .join(changed_new_rows.select(*primary_key_list, version_ts.alias("_new_ts")).alias("n"), primary_key_list, "inner")
        .select("e.*", "_new_ts")
        .withColumn(scd2_end_date_col, F.col("_new_ts"))
        .withColumn(scd2_current_flag_col, F.lit("N"))
        .drop("_new_ts")
    )

    new_current_rows = (
        changed_new_rows.unionByName(brand_new_rows, allowMissingColumns=True)
        .withColumn(scd2_start_date_col, version_ts)
        .withColumn(scd2_end_date_col, F.lit(None).cast(StringType()))
        .withColumn(scd2_current_flag_col, F.lit("Y"))
    )

    existing_max_sk = existing_active.agg(F.max(F.col(surrogate_key_column))).collect()[0][0]
    sk_base = max(existing_max_sk if existing_max_sk is not None else 0, 0)

    sk_window = Window.orderBy(*primary_key_list)
    new_current_rows = (
        new_current_rows
        .withColumn("_rn", F.row_number().over(sk_window))
        .withColumn(surrogate_key_column, F.col("_rn") + F.lit(sk_base))
        .drop("_rn")
        .withColumn("INSERT_DATETIME", F.lit(run_ts))
        .withColumn("UPDATE_DATETIME", F.lit(None).cast("timestamp"))
        .withColumn("SOURCE_INGESTION_LOG_ID", F.lit(source_ingestion_log_id))
        .drop("ModifiedDate")
    )

    all_changes = close_out_rows.unionByName(new_current_rows, allowMissingColumns=True)
    all_changes = all_changes.cache()
    rows_merged = all_changes.count()

    merge_condition = pk_join_condition + f" AND current.{scd2_current_flag_col} = 'Y' AND new.{scd2_current_flag_col} = 'N'"
    target_table = DeltaTable.forName(spark, target_entity)
    (
        target_table.alias("current")
        .merge(all_changes.alias("new"), merge_condition)
        .whenMatchedUpdate(set={
            scd2_current_flag_col: f"new.{scd2_current_flag_col}",
            scd2_end_date_col: f"new.{scd2_end_date_col}",
            "UPDATE_DATETIME": F.lit(run_ts),
            "SOURCE_INGESTION_LOG_ID": F.lit(source_ingestion_log_id),
        })
        .whenNotMatchedInsertAll()
        .execute()
    )
    all_changes.unpersist()

except Exception as e:
    raise Exception(f"[Merge Gold Dimension] Failed to merge into '{target_entity}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = {
    "status": "SUCCESS",
    "rows_read": rows_read,
    "rows_written": rows_merged if rows_read > 0 else 0,
    "rows_rejected": rows_quarantined,
    "watermark_value_used": watermark_value_used,
    "watermark_value_new": (
        str(deduped_data.agg(F.max(F.col(watermark_column))).collect()[0][0])
        if (rows_read > 0 and watermark_column) else watermark_value_used
    ),
}

print(json.dumps(result, indent=2))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("SELECT COUNT(*) AS total_rows FROM dimproduct").show()
spark.sql("SELECT * FROM dimproduct WHERE PRODUCT_SK IN (-1, 1, 2, 3) ORDER BY PRODUCT_SK").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
   SELECT SALESTERRITORY_SK, TerritoryID, Name, IS_CURRENT, SCD_START_DATE, SCD_END_DATE
   FROM dimsalesterritory
   ORDER BY SALESTERRITORY_SK
   """))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
