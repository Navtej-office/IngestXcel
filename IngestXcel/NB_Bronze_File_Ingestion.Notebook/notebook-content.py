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
# ================================================================
# CELL 1 - NOTEBOOK PARAMETERS
# ================================================================
meta_orchestration_id = None
target_workspace_id = None
target_lakehouse_id = None
target_schema_name = None
target_entity = None
processing_method = None
watermark_value_used = None
source_workspace_id = None
source_lakehouse_id = None
wildcard_folder_path = None      # e.g. "landing/excel/sales" (folder, not a glob - see Cell 3)
file_format = None                # 'EXCEL' for now; other values raise in Cell 3
sheet_name = None                 # single sheet, comma list, or '*' for all
file_has_header_row = "true"

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
try:
    if file_format != "EXCEL":
        raise Exception(f"FILE_FORMAT '{file_format}' not yet supported - only EXCEL is implemented so far")

    source_path = (
        f"abfss://{source_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{source_lakehouse_id}/Files/{wildcard_folder_path}"
    )
    folder_path, name_pattern = source_path.rsplit("/", 1) if "*" in source_path.rsplit("/", 1)[-1] else (source_path, "*")

    all_files = notebookutils.fs.ls(folder_path)
    matched = [f for f in all_files if fnmatch.fnmatch(f.name, name_pattern)]

    if processing_method == "INCREMENTAL" and watermark_value_used and watermark_value_used != "1900-01-01":
        cutoff_ms = int(datetime.strptime(watermark_value_used, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
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
# CELL 4 - READ MATCHED EXCEL FILES INTO ONE DATAFRAME
# ================================================================
# pandas can't read abfss:// paths directly - each matched file is copied
# to local temp storage first, read via pandas/openpyxl, then handed to
# Spark. Multiple sheets (incl. '*') get unioned with a sheet_name column
# added, matching the reference framework's own convention; a single
# named sheet does NOT get that extra column. Multiple matched FILES
# also get a _source_file_name column for lineage - this part is our
# own addition, not something the reference framework documents.
try:
    frames = []
    for f in matched:
        local_path = f"/tmp/{f.name}"
        notebookutils.fs.cp(f.path, f"file://{local_path}", True)

        header_row = 0 if str(file_has_header_row).lower() == "true" else None

        if sheet_name == "*":
            sheets = pd.read_excel(local_path, sheet_name=None, header=header_row)
        elif "," in (sheet_name or ""):
            wanted = [s.strip() for s in sheet_name.split(",")]
            sheets = pd.read_excel(local_path, sheet_name=wanted, header=header_row)
        else:
            sheets = {sheet_name: pd.read_excel(local_path, sheet_name=sheet_name, header=header_row)}

        for sname, pdf in sheets.items():
            if len(sheets) > 1:
                pdf["sheet_name"] = sname
            pdf["_source_file_name"] = f.name
            frames.append(pdf)

    if frames:
        combined_pdf = pd.concat(frames, ignore_index=True)
        df = spark.createDataFrame(combined_pdf.astype(object).where(combined_pdf.notnull(), None))
    else:
        df = None

    rows_read = df.count() if df is not None else 0
    watermark_value_new = (
        str(max(f.modifyTime for f in matched) if matched else watermark_value_used)
    )
    # keep as raw epoch ms string for now if no files matched, else format to match watermark_value_used's shape
    if matched:
        watermark_value_new = datetime.fromtimestamp(max(f.modifyTime for f in matched) / 1000).strftime("%Y-%m-%d %H:%M:%S")
except Exception as e:
    raise Exception(f"[Read Excel Files] Failed to read matched files under '{wildcard_folder_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 5 - WRITE TO BRONZE DELTA (same FULL/INCREMENTAL split as
# NB_Bronze_Staged_Ingestion - no key needed either way)
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
        pass  # no files matched at all this run for a FULL entity - nothing to overwrite with; leaving table as-is rather than guessing
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
