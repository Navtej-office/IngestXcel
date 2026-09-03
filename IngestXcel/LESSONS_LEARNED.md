# Lessons Learned

Searchable reference of real bugs hit during this build and their fixes. No chronological
narrative needed — search for a keyword, get the fix. Each entry: what happened, why, the fix.

---

### SQL Server permits duplicate column names in a plain SELECT list
Adding `SOURCE_QUERY_OVERRIDE` to `spGet_Bronze_Batch` initially produced a SELECT list with
`EXECUTION_METHOD` projected twice — an additive diff got pasted alongside the original line
instead of replacing it. T-SQL compiles and runs this without error, silently returning two
same-named columns, which is undefined behavior for a Fabric pipeline expression like
`item().EXECUTION_METHOD`. **Fix:** replace the whole affected SELECT block rather than
appending to it. Check any proc's SELECT list for duplicate output names by eye after an edit —
SQL Server won't warn you.

### Fabric/ADF activity dependencies are always logical AND, never OR
There is no way to make multiple `dependsOn` entries on one activity behave as OR. An activity
meant to fire if *either* of two upstream activities fails must be split into two separate
single-condition activities, not one activity with two dependency entries. Discovered when
`Complete Log Failure_Notebook`'s two-condition dependency silently never fired on a real
Stage-activity failure (the log row stayed stuck at `RUNNING`). **Fix:** split into two
activities (e.g. `Complete Log Failure_Notebook_Stage` and `..._Load`), each with exactly one
dependency condition.

### `EXECUTION_METHOD` cutover must be an in-place UPDATE, never deactivate-old-insert-new
`UIDX_META_ORCHESTRATION` is unique on `(TRIGGER_NAME, SOURCE_ENTITY_ID)`, and
`UIDX_META_CONFIGURATION_CORE` is unique on `(SOURCE_ENTITY_ID, CONFIGURATION_CATEGORY,
CONFIGURATION_NAME)` — neither includes `IS_ACTIVEYN` in the key. The schema hard-enforces
exactly one orchestration row and one `EXECUTION_METHOD` config row per source entity, active or
not; deactivating an old row does not free up the key for a new one. Discovered migrating
`dbo.DimDate` from `PIPELINE` to `NOTEBOOK`: a "deactivate old row, insert a new one" attempt
failed on both unique indexes. **Fix:** an in-place `UPDATE` of the existing
`CONFIGURATION_VALUE` — there is no "keep the old row as historical record" option under this
schema. Rollback is simply running the same `UPDATE` with the prior value.

### An empty-string parameter can still break `STRING_SPLIT`/`CAST` logic
Both `spGet_Bronze_Batch` and `spGet_Silver_Batch` had the same latent bug:
`@TargetEntityIds = '' OR ... IN (SELECT CAST(value AS INT) FROM STRING_SPLIT(@TargetEntityIds,
','))`. SQL Server's `OR` does not guarantee short-circuit evaluation —
`STRING_SPLIT('', ',')` returns **one row** with an empty string (not zero rows), and
`CAST('' AS INT)` on that row throws, even though the first `OR` condition is true. Never
surfaced across many manual "unscoped" runs because leaving a pipeline parameter blank in
Fabric's Run dialog comes through as `NULL`, not a literal empty string, and
`STRING_SPLIT(NULL, ',')` returns zero rows — only surfaced once a chained pipeline invocation
passed a genuine explicit `''`. **Fix:** add `WHERE value <> ''` to the `STRING_SPLIT` subquery
in both procs, filtering the empty-string row out before `CAST` ever sees it.

### Typing quote characters into a UI parameter field sets the value to those literal characters
A related but distinct mistake from the one above: typing `''` or `""` directly into a pipeline
parameter field (thinking it represents SQL's empty-string syntax) makes the field's actual
value those literal quote characters, not zero characters. This produced the exact same
downstream `CAST` failure even after the `STRING_SPLIT` fix above was correctly deployed, since
the proc was now receiving the 2-character string `"''"` or `'""'` rather than a real empty
string. **Fix:** leave the field genuinely, completely blank — no characters at all, not any
quoted representation of "empty."

### A copy-pasted activity can retain a completely different connector type underneath its name
Renaming a pasted activity (e.g. `Get New Watermark` → `Get New Watermark__AzureSQLDB`) does not
change its underlying source type or connection settings — if the original was configured for a
different connector, it silently stays that way. This only surfaces at runtime as a confusing
credential/auth error ("AccessToken not found"), not an obvious configuration mismatch. **Fix:**
after any copy-paste, verify the connector type and connection settings directly, not just the
activity's name and visible parameters — when in doubt, delete and rebuild fresh against a
proven-working connector rather than patching a pasted one.

### There is no distinct "Azure SQL Database" connector in Fabric
Azure-hosted single databases are configured through the generic **SQL Server** connector type,
differentiated only by an Azure server address — not a separately-labeled option in the
connector picker. Azure SQL **Managed Instance** is a different, incompatible connector for a
different deployment model; selecting it instead of SQL Server for a single Azure SQL Database
causes confusing, hard-to-diagnose runtime credential errors rather than a clear "wrong
connector" message.

### Azure Key Vault reference fields must be set via the picker dialog, never typed as plain text
The Password field's AKV-reference icon opens a two-part picker (reference dropdown + separate
secret-name box) — typing text directly into the Password field, even text that looks like a
valid reference, produces a garbled value that fails silently until a real run is attempted,
with an error that doesn't point at the actual cause.

### `NB_Silver_SCD_Load`'s parameters cell had no "parameters" tag at all
The cell's live code (a leftover test block not properly triple-quoted) defaulted to
Person.BusinessEntity's specific SCD1 test values — and since the cell was never tagged as the
notebook's parameters cell, a calling pipeline's real values had nothing to inject into. Would
have silently always reprocessed one hardcoded entity regardless of what the pipeline actually
passed. **Fix:** revert the cell to just `None` defaults as the only live code, then explicitly
toggle it as the parameters cell (right-click → Toggle parameter cell) — a step that's easy to
think you've done via the code content alone, but is a separate Fabric UI action.

### `Get Silver Batch`'s Lookup defaulted to `firstRowOnly=true`, breaking the ForEach
Fabric/ADF's Lookup activity defaults to `firstRowOnly=true` when the setting isn't explicitly
touched — returning one row under `.output.firstRow`, not the array under `.output.value` a
`ForEach` needs. **Fix:** explicitly uncheck "First row only" in the Lookup's settings; don't
assume a setting is correct just because it matches a working sibling activity's *intent*.

### A missing closing bracket in a hand-typed SQL script silently swallowed the whole statement
`EXEC [DBO].[spComplete_Ingestion_Log` (missing `]`) caused SQL Server to treat everything from
that `[` onward — every parameter line — as one giant unclosed bracketed identifier, producing
"identifier too long"/"unclosed quotation mark" errors that don't obviously point at a missing
bracket. **Fix:** the missing `]` right after the procedure name. Worth a close visual check of
hand-typed script text after any manual edit, not just believing an edit "looks right."

### Reconciliation math compared the wrong two things
An early version of Silver's post-write reconciliation check compared `rows_read` against the
deduped row count — which is wrong by design, since dedup is *supposed* to reduce the count
whenever duplicate primary keys exist in a batch. This produced a `MISMATCH` on every single run
where any deduplication happened at all, which is the common case, not an edge case. **Fix:**
compare `rows_merged` against the deduped count instead — confirming everything that survived
dedup actually made it into the target, not that nothing was ever deduplicated.

### Fabric trial capacity allows exactly one concurrent Spark session, with real consequences
Confirmed no bursting, no job queueing — a second session request simply errors out rather than
waiting in line (`TooManyRequestsForCapacity`, HTTP 430). A notebook activity finishing does
**not** mean its Spark session stops immediately in *interactive* mode (calling
`mssparkutils.notebook.exit()` from a manual/interactive run keeps the session alive; only a
*pipeline-invoked* run's `exit()` call actually stops the session automatically). Enabling
"High Concurrency mode for notebooks in pipelines" does not help across medallion layers,
because that feature's automatic session-packing requires matching default Lakehouses between
notebooks — Bronze's and Silver's notebooks structurally can never match, by design. Accepted as
a capacity-tier limitation, not a design defect (see ADR-0001's Consequences).

**A folder/filename split that only triggers on a wildcard silently breaks exact-path entities**
     NB_Bronze_File_Ingestion's file-listing logic originally only split wildcard_folder_path into folder + name-pattern when it detected a * in the last path segment — otherwise it treated the entire path, filename included, as the folder to list, then tried to match * inside it. This worked by coincidence for genuine wildcard patterns but broke completely for an exact single-file path (no wildcard at all), which is a legitimate, common case — fs.ls() was handed a file path as if it were a directory, producing a PathNotFound error with no obvious connection to the real cause. Fix: always split on the last /, unconditionally. fnmatch matches an exact filename correctly on its own — no special-casing needed at all.

**A missing file extension can be invisible in the Fabric GUI — verify via a direct listing**
    A source file's real name had no extension at all, despite metadata assuming .xlsx — but the Fabric portal's Files explorer gave no clear visual indication the extension was actually absent. The authoritative way to confirm a file's real name is a direct notebookutils.fs.ls(folder_path) call in a scratch cell, printing .name for each result — not trusting the portal's file listing at a glance. Fix: when a file-not-found error's message doesn't match visual expectations, list the parent folder programmatically before assuming the metadata or the code is wrong

**Casting every value to string does not guarantee a clean string schema — an all-null column still needs an explicit schema**
    Bronze's file-ingestion design casts every pandas cell to str (or None) specifically to avoid letting Spark infer mixed types. This works for columns with at least one non-null value — but a column that is entirely null across every row in a batch still gets inferred by spark.createDataFrame() as NullType ("void") when no explicit schema is given, since there's no sample value to infer a real type from. Delta/Parquet cannot physically persist a NullType column at all, so it's silently dropped from the written file — while the SQL catalog's metadata still reflects an attempt to write it, producing a schema mismatch that only surfaces later as a confusing query-time error (see the catalog-sync entry below). Fix: build an explicit StructType([StructField(c, StringType(), True) for c in columns]) and pass it directly to spark.createDataFrame(pdf, schema=explicit_schema) — never rely on inference from the data itself, no matter how thoroughly the data was pre-cast.

**A shared column-normalization step can strip a framework-added column's own intentional naming**
    The regex that normalizes source column names (replacing Delta-invalid characters with underscores, then stripping stray leading/trailing underscores) was applied uniformly to every column in the dataframe — including _source_file_name, a lineage column the framework itself adds, not something read from the source file. The trailing .strip('_') call stripped its intentional leading underscore too, silently renaming it to source_file_name and erasing the visual distinction between framework-added metadata and genuine source columns. Fix: exclude the framework's own added column names by name from the normalization/strip step entirely, rather than trying to make the regex "smart" about which underscores are intentional.

**A table created via a direct path write can leave the SQL catalog out of sync with the real Delta log**
    Deliberately testing Bronze's lazy-create fallback (writing to a target path with no table pre-created via DDL) confirmed the write itself never crashes — mode("append")/mode("overwrite") to a nonexistent Delta path creates the table cleanly. But querying that table afterward via its three-part SQL catalog name (SELECT * FROM lakehouse.schema.table) threw IllegalStateException: Couldn't find X#N in [...] — the catalog's cached schema had gone stale relative to the table's actual Delta log, confirmed by comparing DESCRIBE TABLE output against a direct spark.read.format("delta").load(path).printSchema(), which showed a genuinely different column list. This is a known class of Spark/Delta bug tied specifically to tables registered via a direct path write rather than an explicit CREATE TABLE, and it's concrete, observed evidence for why DDL-first (ADR-0002) matters in practice, not just as a governance preference. Fix when hit: drop the lazily-created table and recreate it properly via explicit DDL, then rerun — spark.catalog.refreshTable(...) is worth trying first as a cheaper possible fix, but isn't guaranteed to resolve it

**Storing a file-modification watermark as a formatted date string loses precision needed for correctness**
    The file-based watermark mechanism originally stored watermark_value_new via datetime.fromtimestamp(mtime_ms / 1000).strftime("%Y-%m-%d %H:%M:%S") — second-level precision only. Reconstructing it back to epoch milliseconds on the next run always read as exactly .000, permanently less than the file's real (almost always non-zero-millisecond) modification time. This made the > comparison in the incremental filter true on every subsequent run, even when the file never changed — a watermark that could never catch up to itself, defeating idempotency entirely. Fix: store and compare the raw epoch-millisecond integer directly (str(mtime_ms), int(watermark_value_used)) — no datetime formatting round-trip at all. A second-order mistake worth naming directly: the first attempted fix left the old lossy strftime line in place alongside the new correct line, so it silently overwrote the correct value every run — proven only by the exit-value output still showing the old date-string format after the "fix" was believed applied. When fixing a bug that spans several lines, replace the whole affected block and re-verify the actual output, rather than trusting that adding a new line beside an old one is equivalent to removing the old one.

**A Switch case's match value is a separate field from its activity's display name**
    Adding a new Switch case and naming its Notebook activity clearly (e.g. Load Lakehouse File to Bronze) does not set the case's actual match value — that's a distinct field, and it's easy to set it to the activity's name by mistake instead of the real EXECUTION_METHOD_SOURCE_TECH_TYPE string the Switch expression produces. The failure is silent and unhelpful: the Switch's own logged output correctly shows the expression it evaluated, but since no case value matches it, execution falls through to Fail_default_case with a generic "unsupported combination" message that gives no hint the case even exists. Fix: verify the case's value field directly against the exact string the Switch expression evaluates to — don't assume it was set correctly just because the activity inside the case has an accurate name.

**A Lakehouse can temporarily refuse to attach as a notebook's default while a sibling Lakehouse works fine**
    One Lakehouse briefly could not be set as default in any notebook — including a freshly created one — while a second Lakehouse in the same workspace attached without issue. This ruled out anything specific to a particular notebook. It resolved itself after closing all browser windows/tabs and reconnecting to Fabric fresh, suggesting a stale session or auth-token state tied to that specific item, not a stuck transaction (Delta's own transaction log has no bearing on notebook-attach at all, which is a workspace/item-metadata operation) or genuine item corruption. Fix: if a specific Lakehouse won't attach as default anywhere, try a full session reset (close everything, reconnect) before assuming the item itself needs repair or recreation.

**Toggling a tenant setting off then back on can clear a stuck permission-propagation error**
    After dropping a paid Fabric capacity and remapping a workspace back to Trial, an "item type needs to be turned on in the Admin portal" error persisted even though the relevant tenant setting (Users can create Fabric items) already showed as enabled. Turning the setting off, then back on, resolved it immediately — indicating the error was a stale propagation/cache state rather than an actual configuration problem, which simply re-toggling the value forced to refresh. Fix: if a tenant setting shows enabled but its corresponding error persists, try disabling and re-enabling it once before escalating further or assuming a deeper capacity issue.

**Fabric SQL Database mirroring skips computed columns, and Direct Lake on SQL doesn't support DAX calculated columns either**
    Added DATE_KEY to META_INGESTION_LOG as a persisted computed column (`AS (CONVERT(INT, CONVERT(VARCHAR(8), RUN_START_DT, 112))) PERSISTED`) so a Direct Lake semantic model could relate the log table to a new DATE_DIMENSION for Power BI reporting. The column applied cleanly and queried correctly at the plain T-SQL level (SELECT TOP 5 showed correct values), but it never appeared in the semantic model's column list for that table, even after refreshing the model and the SQL analytics endpoint. Adding a second, plain (non-computed) date column as a side-by-side test proved the cause: the plain column showed up in the model immediately, the computed one never did. Confirmed against Microsoft's own documentation -- Fabric SQL Database mirroring explicitly skips computed columns regardless of PERSISTED (learn.microsoft.com/fabric/database/sql/mirroring-limitations), and deriving the column with a DAX calculated column instead isn't a usable workaround either, because this model uses Direct Lake on SQL (via the SQL analytics endpoint), which doesn't support DAX calculated columns at all -- only the separate "Direct Lake on OneLake" mode supports them, and only in preview. Fix: any derived/reporting column that needs to reach a Direct Lake semantic model must be a real physical column populated by application code, not a SQL computed column or a DAX calculated column -- DATE_KEY is now a plain INT column set explicitly by spStart_Ingestion_Log alongside RUN_START_DT.
