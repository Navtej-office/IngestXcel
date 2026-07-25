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
