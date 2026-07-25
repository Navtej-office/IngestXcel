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

# PARAMETERS CELL ********************

# ================================================================
# CELL 1 - NOTEBOOK PARAMETERS
# ================================================================
#
# Purpose
# -------
# This cell defines all input parameters required by the notebook.
# These parameters are typically supplied by the parent Fabric spGet_Bronze_Batch
# pipeline using Notebook activity parameters.

# ================================================================

meta_orchestration_id = None
staging_path = None
target_workspace_id = None
target_lakehouse_id = None
target_schema_name = None
target_entity = None
processing_method = None
watermark_column = None
watermark_value_used = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 2 - IMPORTS
# ================================================================
#
# Purpose
# -------
# json         - builds the exit-value JSON string for
#                mssparkutils.notebook.exit() (Cell 6).
# F (functions)- PySpark column operations; used in Cell 4 to
#                compute MAX(watermark_column) via F.max(F.col(...)).
# StringType   - casts the datetime watermark value to a plain
#                string, since the exit-value contract (JSON) has
#                no native datetime type.
#
# No JDBC / Spark-SQL-connector library needed here — the source
# read already happened upstream in the pipeline's Copy activity
# (staged to Parquet). This notebook only reads Parquet + writes
# Delta, both native to the Fabric Spark runtime. See CLAUDE.md
# item #30.
# ================================================================

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 3 - READ STAGED PARQUET
# ================================================================
#
# Purpose
# -------
# Reads the batch the pipeline's "Stage Fabric SQLDB Source" Copy
# activity already staged to Parquet (item #30) and counts the rows.
# Any needed source-side type conversion (e.g. SpatialLocation
# .STAsText() for Person.Address) already happened upstream in that
# Copy activity's query - this cell does no source-specific logic,
# just a plain Parquet read.
#
# rows_read == 0 is a valid, non-error outcome (nothing past the
# watermark this run) - Cell 4 already handles that case correctly,
# so we don't special-case it here.
#
# Error handling (CLAUDE.md item #33): on failure, re-raise with a
# short step-tagged message instead of letting a raw Spark stack
# trace surface. The exception is chained (`from e`) so the original
# detail isn't lost, and it's still allowed to propagate uncaught -
# the cell must fail so the Notebook activity reports Failed and
# Complete Log Failure_Notebook can read the error message downstream
# (item #32).
# ================================================================

try:
    df = spark.read.parquet(staging_path)
    rows_read = df.count()
except Exception as e:
    raise Exception(f"[Read Staged Parquet] Failed to read staged data from '{staging_path}': {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# CELL 4 - COMPUTE NEW WATERMARK
# ================================================================
#
# Purpose
# -------
# Determines what watermark_value_new should be, based on what was
# actually read in Cell 3 - this is the value Complete Log Success
# will persist as the entity's new watermark for the next run.
#
# Two cases where the watermark stays UNCHANGED (not an error):
#   - rows_read == 0        -> nothing past the last watermark this
#                               run; nothing to advance to.
#   - processing_method !=
#     'INCREMENTAL' (i.e. FULL) -> FULL loads don't track a
#                               watermark at all.
#
# Otherwise (INCREMENTAL with rows_read > 0): take MAX(watermark_column)
# over the batch already in memory - no second round trip to the
# source, unlike the PIPELINE cases' separate "Get New Watermark"
# Lookup (item #30's deliberate design choice). Cast to StringType
# since the exit-value JSON contract (item #32) has no native
# datetime type.
#
# Error handling (item #33): same step-tagged, re-raised, chained
# exception pattern as Cell 3.
# ================================================================

try:
    if rows_read == 0 or processing_method != "INCREMENTAL":
        watermark_value_new = watermark_value_used
    else:
        watermark_value_new = str(
            df.agg(F.max(F.col(watermark_column)).cast(StringType())).collect()[0][0]
        )
except Exception as e:
    raise Exception(f"[Compute New Watermark] Failed to compute MAX({watermark_column}): {e}") from e

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

try:
    target_path = (
        f"abfss://{target_workspace_id}@onelake.dfs.fabric.microsoft.com/"
        f"{target_lakehouse_id}/Tables/{target_schema_name}/{target_entity}"
    )

    if rows_read > 0:
        (
            df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(target_path)
        )

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
# CELL 6 - RETURN EXIT-VALUE CONTRACT TO PIPELINE
# ================================================================
#
# Purpose
# -------
# Final cell - hands results back to the pipeline via
# mssparkutils.notebook.exit(). This is the standing contract
# (CLAUDE.md item #32) every NOTEBOOK-method notebook must follow,
# at every medallion layer, so Complete Log Success_Notebook can
# read it with @json(activity('Load Staged Parquet to Bronze')
# .output.result.exitValue).<field> without any per-layer branching.
#
# rows_rejected is always 0 here - Bronze is deliberately
# "land raw, don't validate" (no DQ/quarantine logic at this layer;
# that's a Silver-layer concern, see item #37).
#
# mssparkutils.notebook.exit() requires a JSON STRING, not a dict -
# hence json.dumps() (this is what Cell 2's `import json` was for).
#
# Nothing in this cell is wrapped in try/except: if we've reached
# this point, Cells 3-5 all succeeded, so there's nothing left that
# should legitimately fail. If it somehow does, letting it raise
# uncaught is still correct per item #32/#33 - Complete Log
# Failure_Notebook must still see the Notebook activity as Failed.
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
