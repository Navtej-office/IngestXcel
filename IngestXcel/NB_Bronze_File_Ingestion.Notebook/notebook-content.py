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
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# PARAMETERS CELL ********************

meta_orchestration_id = 11
target_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"
target_lakehouse_id = "a6ee5274-a432-4395-b3a2-4f18886fce18"
target_schema_name = "ingestxcel_files"
target_entity = "book_catalog"
processing_method = "INCREMENTAL"
watermark_value_used = "1900-01-01"
source_workspace_id = "2773bec8-6438-4872-b2ee-d34f1a32b3a9"
source_lakehouse_id = "a6ee5274-a432-4395-b3a2-4f18886fce18"
wildcard_folder_path = "book_catalog/book_catalog_v*.csv"
file_format = "CSV"
sheet_name = None
file_has_header_row = "true"
delimiter = ","

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 2 - IMPORTS
# ================================================================
import json
import fnmatch
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# CELL 3 - LIST & FILTER MATCHING FILES BY MODIFICATION TIME
# ================================================================
# No column-based watermark exists for files - incremental is driven by
# file modification time instead, checked before any file is opened.
# wildcard_folder_path is a FOLDER; the file-name pattern (e.g. "*.xlsx")
# is matched here via fnmatch, not passed to fs.ls itself.
# ================================================================
# CELL 3 - LIST & FILTER MATCHING FILES BY MODIFICATION TIME
# ================================================================
try:
    if file_format not in ("EXCEL", "CSV"):
        raise Exception(f"FILE_FORMAT '{file_format}' not yet supported - only EXCEL and CSV are implemented so far")

    source_path = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Files/{wildcard_folder_path}"
    )
    folder_path, name_pattern = source_path.rsplit("/", 1)
    all_files = notebookutils.fs.ls(folder_path)
    matched = [f for f in all_files if fnmatch.fnmatch(f.name, name_pattern)]

    if processing_method == "INCREMENTAL" and watermark_value_used and watermark_value_used != "1900-01-01":
        cutoff_ms = int(watermark_value_used)
        matched = [f for f in matched if f.modifyTime > cutoff_ms]

    rows_read = 0  # computed after reading, in Cell 4
except Exception as e:
    raise Exception(f"[List Source Files] Failed to list '{wildcard_folder_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 4 - READ MATCHED FILES INTO ONE DATAFRAME
# ================================================================
import re
from pyspark.sql.types import StructType, StructField, StringType

try:
    frames = []
    for f in matched:
        local_path = f"/tmp/{f.name}"
        notebookutils.fs.cp(f.path, f"file://{local_path}", True)

        header_row = 0 if str(file_has_header_row).lower() == "true" else None

        if file_format == "EXCEL":
            if sheet_name == "*":
                sheets = pd.read_excel(local_path, sheet_name=None, header=header_row, engine="openpyxl")
            elif "," in (sheet_name or ""):
                wanted = [s.strip() for s in sheet_name.split(",")]
                sheets = pd.read_excel(local_path, sheet_name=wanted, header=header_row, engine="openpyxl")
            else:
                sheets = {sheet_name: pd.read_excel(local_path, sheet_name=sheet_name, header=header_row, engine="openpyxl")}

            for sname, pdf in sheets.items():
                if len(sheets) > 1:
                    pdf["sheet_name"] = sname
                pdf["_source_file_name"] = f.name
                frames.append(pdf)

        elif file_format == "CSV":
            pdf = pd.read_csv(local_path, header=header_row, sep=delimiter, dtype=str, keep_default_na=True)
            pdf["_source_file_name"] = f.name
            frames.append(pdf)

        else:
            raise Exception(f"FILE_FORMAT '{file_format}' not yet supported in Cell 4's read logic")

    if frames:
        combined_pdf = pd.concat(frames, ignore_index=True)

        # Normalize real source column names only - never touch our own
        # framework-added lineage columns' names.
        combined_pdf.columns = [
            c if c in ("_source_file_name", "sheet_name") else re.sub(r'[ ,;{}()\n\t=]+', '_', str(c)).strip('_')
            for c in combined_pdf.columns
        ]

        for col in combined_pdf.columns:
            combined_pdf[col] = combined_pdf[col].apply(lambda x: None if pd.isnull(x) else str(x))

        # Force every column to StringType explicitly - letting Spark infer
        # from the pandas frame is NOT reliable when a column is entirely
        # null (infers as NullType/void, which Delta/Parquet can't persist).
        explicit_schema = StructType([StructField(c, StringType(), True) for c in combined_pdf.columns])
        df = spark.createDataFrame(combined_pdf, schema=explicit_schema)
    else:
        df = None

    rows_read = df.count() if df is not None else 0
    watermark_value_new = str(max(f.modifyTime for f in matched)) if matched else watermark_value_used
except Exception as e:
    raise Exception(f"[Read Files] Failed to read matched files under '{wildcard_folder_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 5 - WRITE TO BRONZE DELTA
# ================================================================
try:
    target_path = (
        f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_lakehouse_id}/Tables/{target_schema_name}/{target_entity}"
    )
    if df is not None and rows_read > 0:
        mode = "overwrite" if processing_method == "FULL" else "append"
        df.write.format("delta").mode(mode).option("mergeSchema", "true").save(target_path)
    elif processing_method == "FULL" and df is None:
        pass  # no files matched this run for a FULL entity - leave table as-is
    rows_written = rows_read
except Exception as e:
    raise Exception(f"[Write Bronze Delta] Failed to write to '{target_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 6 - EXIT-VALUE CONTRACT
# ================================================================
result = {
    "status": "SUCCESS",
    "rows_read": rows_read,
    "rows_written": rows_written,
    "rows_rejected": 0,
    "watermark_value_used": watermark_value_used,
    "watermark_value_new": watermark_value_new,
}
mssparkutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
