# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# NB_Silver_SCD_Load
# 
#  Reads incremental rows from a Bronze Delta table, dedups, and merges into
#  Silver via either SCD1 (simple upsert) or SCD2 (historized). Pattern for
# the SCD2 branch adapted directly from ISD Accelerator's proven
# hash-comparison + single-merge approach (see chat for the full walkthrough) — not invented from scratch.


# CELL ********************

# ================================================================
# CELL 1 - PARAMETERS
# ================================================================
# Toggle this cell as the notebook's parameters cell in the Fabric UI.
# Values below are placeholders; the pipeline overrides them at runtime.
"""
meta_orchestration_id = None
source_workspace_id = None
source_lakehouse_id = None
source_schema_name = None   # Bronze's schema folder - see chat note, proc needs a follow-up fix to supply this
source_entity = None        # Bronze table name, e.g. "person_address"
target_workspace_id = None
target_lakehouse_id = None
target_entity = None        # Silver table name, e.g. "person_address"
primary_keys = None         # comma-separated, e.g. "AddressID"
watermark_column = None     # e.g. "ModifiedDate"
watermark_value_used = None
scd_type = None             # "SCD1" or "SCD2"
scd2_start_date_col = None  # only meaningful when scd_type = "SCD2"
scd2_end_date_col = None
scd2_current_flag_col = None
quarantine_table_name = None
"""

meta_orchestration_id = 26
source_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"
source_lakehouse_id = "7e491b48-5978-4a73-bbe7-b98df9812e65"
source_schema_name = "fabrictraining_ingestxcel"
source_entity = "person_address"
target_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"
target_lakehouse_id = "0d793c52-9170-4eb5-b0b4-6bdf137c4403"
target_entity = "person_address"
primary_keys = "AddressID"
watermark_column = "ModifiedDate"
watermark_value_used = "1900-01-01"
scd_type = "SCD2"
scd2_start_date_col = "ModifiedDate"
scd2_end_date_col = "SCD_END_DATE"
scd2_current_flag_col = "IS_CURRENT"
quarantine_table_name = "person_address_quarantine"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# DeltaTable - needed for the merge() API (both SCD1 and SCD2).
# functions/Window - hash-based dedup and change detection.
# json - exit-value contract, same as the Bronze notebook.
 
import json
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Reads Bronze's Delta table directly (not staged Parquet - Bronze is
# already Delta, no staging step needed for a Delta-to-Delta read).
# Filters to rows past Silver's own last-processed watermark - this is
# Silver's own incremental progress against Bronze, separate from
# Bronze's own watermark against the original external source.
 
try:
    primary_key_list = [k.strip() for k in primary_keys.split(",")]
 
    bronze_path = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Tables/{source_schema_name}/{source_entity}"
    )
 
    bronze_df = spark.read.format("delta").load(bronze_path)
    new_data = bronze_df.filter(F.col(watermark_column) > F.lit(watermark_value_used))
    rows_read = new_data.count()
except Exception as e:
    raise Exception(f"[Read Bronze Source] Failed to read '{bronze_path}': {e}") from e
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Resolves to one row per primary key, keeping the latest by watermark
# value (Must-Have #2). Rows with a NULL primary key can't be merged
# meaningfully - quarantined here rather than allowed to break the merge.
 
try:
    if rows_read > 0:
        null_pk_condition = " OR ".join([f"{k} IS NULL" for k in primary_key_list])
        quarantined_null_pk = new_data.filter(null_pk_condition)
        new_data_valid = new_data.filter(f"NOT ({null_pk_condition})")
 
        dedup_window = Window.partitionBy(*primary_key_list).orderBy(F.col(watermark_column).desc())
        deduped_data = (
            new_data_valid
            .withColumn("_dedup_rank", F.row_number().over(dedup_window))
            .filter(F.col("_dedup_rank") == 1)
            .drop("_dedup_rank")
        )
        rows_quarantined = quarantined_null_pk.count()
    else:
        deduped_data = new_data
        quarantined_null_pk = new_data.limit(0)  # empty, correct schema
        rows_quarantined = 0
except Exception as e:
    raise Exception(f"[Dedup Batch] Failed to dedup on [{primary_keys}]: {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# CELL 5 - WRITE QUARANTINED ROWS
# ================================================================
# Physically persists quarantined rows (not just a count) so they're
# inspectable later (Must-Have #7).
 
try:
    if rows_quarantined > 0:
        quarantine_path = (
            f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
            f"{target_lakehouse_id}/Tables/{quarantine_table_name}"
        )
        (
            quarantined_null_pk.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(quarantine_path)
        )
except Exception as e:
    raise Exception(f"[Write Quarantine] Failed to write to quarantine table: {e}") from e
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


 
# ================================================================
# CELL 6 - MERGE INTO SILVER (branches on SCD_TYPE)
# ================================================================
# SCD1: straightforward upsert - whenMatchedUpdateAll/whenNotMatchedInsertAll.
#
# SCD2: adapted directly from ISD Accelerator's proven pattern -
#   1. Load existing Silver rows where {current_flag_col} = 'Y' (active only).
#   2. Hash-compare business columns (excluding PK and the timestamp/
#      watermark column, which change every batch regardless) between new
#      and existing active rows. Differing hash = changed; no existing
#      match = brand new; matching hash = unchanged, no action at all.
#   3. Build three row-sets: (a) the OLD version of changed rows, stamped
#      end_date=new timestamp, current_flag='N' - closes it out; (b) the
#      NEW version of changed rows, start_date=new timestamp, end_date=NULL,
#      current_flag='Y'; (c) brand-new rows, same current_flag='Y' shape.
#   4. Union all three, then ONE merge: match condition is
#      PK AND current.[current_flag]='Y' AND new.[current_flag]='N' - this
#      only matches the close-out rows against their active counterpart.
#      whenMatchedUpdate flips current_flag/end_date on that one row;
#      everything else (new versions, brand-new) doesn't match and falls
#      through to whenNotMatchedInsertAll.
#
# First-run bootstrap: if the Silver table doesn't exist yet, every deduped
# row is simply written as a brand-new current version - no merge needed.
 
try:
    target_path = (
        f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_lakehouse_id}/Tables/{source_schema_name}/{target_entity}"
    )
 
    pk_join_condition = " AND ".join([f"current.`{k}` <=> new.`{k}`" for k in primary_key_list])
    table_exists = DeltaTable.isDeltaTable(spark, target_path)
 
    if scd_type == "SCD1":
        if not table_exists:
            deduped_data.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(target_path)
        else:
            target_table = DeltaTable.forPath(spark, target_path)
            (
                target_table.alias("current")
                .merge(deduped_data.alias("new"), pk_join_condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        rows_merged = deduped_data.count()
 
    elif scd_type == "SCD2":
        if not table_exists:
            # Bootstrap: everything is a brand-new current version
            initial_load = (
                deduped_data
                .withColumn(scd2_start_date_col, F.col(watermark_column))
                .withColumn(scd2_end_date_col, F.lit(None).cast(StringType()))
                .withColumn(scd2_current_flag_col, F.lit("Y"))
            )
            initial_load.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(target_path)
            rows_merged = initial_load.count()
        else:
            existing_active = (
                spark.read.format("delta").load(target_path)
                .filter(F.col(scd2_current_flag_col) == "Y")
            )
 
            hash_exclude_cols = set(primary_key_list) | {watermark_column, scd2_start_date_col, scd2_end_date_col, scd2_current_flag_col}
            new_hash_cols = sorted([c for c in deduped_data.columns if c not in hash_exclude_cols])
            existing_hash_cols = sorted([c for c in existing_active.columns if c not in hash_exclude_cols])
 
            new_hashed = deduped_data.select(*primary_key_list, watermark_column, F.hash(*new_hash_cols).alias("new_hash"))
            existing_hashed = existing_active.select(*primary_key_list, F.hash(*existing_hash_cols).alias("existing_hash"))
 
            compared = new_hashed.join(existing_hashed, primary_key_list, "left_outer")
            changed_keys = compared.filter(
                (F.col("existing_hash").isNotNull()) & (F.col("new_hash") != F.col("existing_hash"))
            ).select(*primary_key_list).distinct()
            new_keys = compared.filter(F.col("existing_hash").isNull()).select(*primary_key_list).distinct()
 
            changed_new_rows = deduped_data.join(changed_keys, primary_key_list, "inner")
            brand_new_rows = deduped_data.join(new_keys, primary_key_list, "inner")
 
            # Close out the old version of changed rows - join in the new
            # row's watermark value per-key to use as this row's end date.
            close_out_rows = (
                existing_active.join(changed_keys, primary_key_list, "inner").alias("e")
                .join(changed_new_rows.select(*primary_key_list, F.col(watermark_column).alias("_new_ts")).alias("n"), primary_key_list, "inner")
                .select("e.*", "_new_ts")
                .withColumn(scd2_end_date_col, F.col("_new_ts"))
                .withColumn(scd2_current_flag_col, F.lit("N"))
                .drop("_new_ts")
            )
 
            new_version_rows = (
                changed_new_rows
                .withColumn(scd2_start_date_col, F.col(watermark_column))
                .withColumn(scd2_end_date_col, F.lit(None).cast(StringType()))
                .withColumn(scd2_current_flag_col, F.lit("Y"))
            )
 
            brand_new_final = (
                brand_new_rows
                .withColumn(scd2_start_date_col, F.col(watermark_column))
                .withColumn(scd2_end_date_col, F.lit(None).cast(StringType()))
                .withColumn(scd2_current_flag_col, F.lit("Y"))
            )
 
            all_changes = close_out_rows.unionByName(new_version_rows, allowMissingColumns=True).unionByName(brand_new_final, allowMissingColumns=True)
 
            merge_condition = pk_join_condition + f" AND current.{scd2_current_flag_col} = 'Y' AND new.{scd2_current_flag_col} = 'N'"
            target_table = DeltaTable.forPath(spark, target_path)
            (
                target_table.alias("current")
                .merge(all_changes.alias("new"), merge_condition)
                .whenMatchedUpdate(set={scd2_current_flag_col: f"new.{scd2_current_flag_col}", scd2_end_date_col: f"new.{scd2_end_date_col}"})
                .whenNotMatchedInsertAll()
                .execute()
            )
            rows_merged = all_changes.count()
    else:
        raise Exception(f"Unrecognized SCD_TYPE '{scd_type}' - expected 'SCD1' or 'SCD2'")
 
except Exception as e:
    raise Exception(f"[Merge Silver] Failed to merge into '{target_path}': {e}") from e
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# CELL 7 - POST-WRITE ROW-COUNT RECONCILIATION
# ================================================================
# Should-Have #9: compare rows processed vs. rows actually merged, log a
# warning (not blocking) if they don't match. A mismatch here doesn't fail
# the run - it's a signal to investigate, not a hard stop.
 
try:
    expected_rows = deduped_data.count()
    reconciliation_status = "OK" if rows_merged == expected_rows else "MISMATCH"
    reconciliation_note = f"expected(deduped_count)={expected_rows}, rows_merged={rows_merged}"
except Exception as e:
    reconciliation_status = "ERROR"
    reconciliation_note = str(e)
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# CELL 8 - RETURN EXIT-VALUE CONTRACT TO PIPELINE
# ================================================================
# Same standing contract as Bronze (CLAUDE.md item #32): status, rows_read,
# rows_written, rows_rejected, watermark_value_used, watermark_value_new.
# rows_written here = rows_merged (Silver's equivalent of "rows affected at
# the target" - matches item #32's definition for a merge, not just append).
 
result = {
    "status": "SUCCESS",
    "rows_read": rows_read,
    "rows_written": rows_merged if rows_read > 0 else 0,
    "rows_rejected": rows_quarantined,
    "watermark_value_used": watermark_value_used,
    "watermark_value_new": (
        str(deduped_data.agg(F.max(F.col(watermark_column))).collect()[0][0])
        if rows_read > 0 else watermark_value_used
    ),
    "reconciliation_status": reconciliation_status,
    "reconciliation_note": reconciliation_note,
}
 
mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

