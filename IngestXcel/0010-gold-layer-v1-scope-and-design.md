# ADR-0010: Gold Layer v1 Scope & Design

**Status:** Proposed (design only — no notebook/pipeline/DDL code written yet)

## Context

`ADR-0006` established the Gold placeholder and one standing principle: Gold reads from
Silver's own target, never from the original source. `PL_GOLD` itself was never built — only
referenced in that ADR. Bronze and Silver are both proven for the tables this v1 build needs:
`Production.Product`, `Sales.Customer`, `Sales.SalesTerritory`, `Sales.SalesOrderHeader`, and
`Sales.SalesOrderDetail` are all confirmed loaded through Silver (`product`, `sales_customer`,
`sales_salesterritory`, `sales_salesorderheader`, `sales_salesorderdetail`).

The goal for v1 is a minimal, genuinely working star schema — one fact and four dimensions —
built with only the features that specific scope actually needs, not a general-purpose Gold
engine. Capabilities that a mature enterprise Gold-layer implementation would eventually need,
but that this scope doesn't require yet, are listed at the end as a deferred backlog, described
generically rather than attributed to a specific named reference — consistent with how the rest
of this repo's documentation now describes borrowed patterns.

## Decision: v1 Feature Scope

**Star schema:**
- Fact: `factsalesorderdetail` — one row per order line, joining
  `sales_salesorderheader` (order-level attributes: order date, customer, territory) with
  `sales_salesorderdetail` (line-level attributes: product, quantity, unit price, line total).
  This is a **transactional fact table** — grain is the individual order-line business event, not
  a periodic snapshot. Natural/degenerate key: `(SalesOrderID, SalesOrderDetailID)`.
- Dimensions: `dimproduct`, `dimcustomer`, `dimsalesterritory` — all **SCD2** (surrogate key,
  start/end date, current flag), reusing the exact hash-compare algorithm already proven in
  Silver (ADR-0007), applied one layer further down.
- `dimdate` is a deliberate exception to "all dimensions SCD2": a calendar dimension has no
  concept of a changing attribute to historize — `2026-09-10` doesn't get a new version. It's
  built once via a static date-range generator (not sourced from Silver, not driven by
  `META_SOURCE_ENTITY`/watermark machinery at all) and is excluded from the recurring
  `PL_Gold_Ingestion` trigger entirely — regenerating it isn't a per-run concern. Flagging this
  explicitly since it departs from the stated "all dimensions SCD2" rule; happy to force SCD2
  columns onto it anyway for schema uniformity if you'd rather keep the four dimensions
  structurally identical.

**Surrogate keys:** no new control table. Each dimension notebook reads
`MAX(surrogate_key)` from its own Gold target table before assigning new keys — via
`row_number()` offset by that max — to genuinely new or changed rows only. Existing rows never
get a new key on a re-run, which matters because Fact rows hold these keys permanently as
foreign keys; reassigning them on replay would silently orphan history.

**Idempotency / re-processing:**
- Dimensions: identical to Silver's SCD2 — no existing match = brand new; hash differs = new
  version (old row closed out, new row opened); hash matches = no-op. Re-running with unchanged
  source data produces zero new rows.
- Fact: plain `MERGE` upsert on the natural key `(SalesOrderID, SalesOrderDetailID)` —
  `whenMatchedUpdateAll` / `whenNotMatchedInsertAll`. No historization on the fact itself (only
  dimensions get SCD treatment); this is what makes "the same record comes again" and "a
  corrected record comes again" both resolve cleanly to one row.

**Dimension key resolution for the fact (known v1 simplification):** the fact load resolves
each dimension's surrogate key by joining against that dimension's *currently active*
(`IS_CURRENT='Y'`) row for the matching natural key, at the time Gold processes the batch — not
a historically-accurate "the key that was active as of the transaction's own date" resolution
(which would need a range-join against each dimension's SCD2 start/end dates). Called out as a
deliberate v1 shortcut, not a silent gap — see backlog.

**Late-arriving dimension fallback:** if a fact row's natural key isn't found in a dimension
yet (e.g. a genuinely new customer that hasn't flowed through Silver/Gold yet), v1's behavior is
to quarantine that fact row — reusing the same quarantine-table mechanism and philosophy Silver
already established — rather than inserting a null/placeholder key or failing the whole batch.
Auto-generating an inferred-member dimension row instead is deferred (see backlog).

**Source join for the fact (known v1 simplification):** because the fact spans two Silver
source tables (header + detail), and v1 has exactly one fact table, `NB_Gold_Fact_Load` hardcodes
that one join rather than introducing a generalized metadata-driven multi-source-join
declaration. If a second multi-source fact is added later, this is the point to generalize —
flagged now rather than building an unused abstraction today.

## Decision: Row-Level Audit & Lineage Columns

Every Gold table (dimensions and fact alike) gets three additional bookkeeping columns, named
in the same upper-snake-case style as the existing SCD2 bookkeeping columns:

- **`INSERT_DATETIME`** — system timestamp set only in the `whenNotMatchedInsert` branch of the
  merge; never touched again after that row is inserted.
- **`UPDATE_DATETIME`** — system timestamp set only in the `whenMatchedUpdate` branch. On a
  dimension this also fires when an existing version gets closed out (`IS_CURRENT` flips to
  `'N'`), capturing "when did the pipeline touch this row" — a different signal from
  `SCD_END_DATE`, which records when the version stopped being *business*-true and is often
  derived from source data rather than system time. Deliberately two different timestamps, not a
  duplicate of what SCD2 already provides.
- **`SOURCE_INGESTION_LOG_ID`** — the `META_INGESTION_LOG` row ID of the run that last
  inserted/updated this row. Stored as a plain value, **no enforced FK**, matching Principle #17
  (the log table stays FK-free to avoid resolving it at execution time). Gives full backtracking
  from any Gold row to the exact run's status/timing/row counts — the more common real debugging
  need — without joining through timestamp ranges.

**Deliberately not doing:** a full `SOURCE_SYSTEM_NAME`/`SOURCE_SYSTEM_KEY`/`SOURCE_SYSTEM_VALUE`
triplet on every row. That pattern earns its cost once a single Gold table can be fed by *more
than one* source system — at that point per-row lineage is essential because which system a row
came from genuinely varies row to row. Every Gold table in this v1 scope maps 1:1 to exactly one
Silver entity from exactly one source system; that mapping is a static fact already recorded in
`META_SOURCE_ENTITY`/`META_ORCHESTRATION` and in this ADR, not something that differs per row.
Backtracking to the originating Silver row doesn't need new columns either — every dimension
already retains its natural key (needed for the SCD2 merge) and the fact retains its own natural
key `(SalesOrderID, SalesOrderDetailID)` (needed for the upsert); combined with the static
table-to-entity mapping, that's already a complete trace path. Full per-row source-system lineage
is listed in the backlog below for if/when a Gold table genuinely becomes multi-source.

## Decision: Metadata Design

No new tables and no schema changes — every piece of this reuses existing, mostly-unused
metadata surface:

- Gold entities get their own `META_SOURCE_ENTITY` row each, `SOURCE_CONNECTION_ENDPOINT_ID`
  pointing at Silver's own target endpoint (`TGT_INGESTXCEL_SILVER_LH`) — the same "this layer
  reads the prior layer's own target" pattern Silver already uses against Bronze, and exactly
  what ADR-0006 already committed to.
- Each Gold entity also gets its own `META_ORCHESTRATION` row, per the established "every layer
  transition is its own orchestration row" convention — under a new per-system trigger,
  `TRG_FABRICTRAINING_INGESTXCEL_GOLD_LOAD_DAILY`, same `SystemIdentifier` as this data's
  existing Bronze/Silver triggers.
- **`ORDER_OF_OPERATIONS`** (existing column on `META_ORCHESTRATION`, unused until now) is
  populated for Gold entities as a gapped wave number: `10` for the three sourced dimensions,
  `20` for the fact. Gapped deliberately so a future wave can be inserted between them without
  renumbering everything.
- `META_CONFIGURATION_CORE`, category `GOLD`: `SCD_TYPE='SCD2'`, and the same
  `SCD2_START_DATE_COL`/`SCD2_END_DATE_COL`/`SCD2_CURRENT_FLAG_COL`/`QUARANTINE_TABLE_NAME`
  vocabulary Silver already established, reused verbatim rather than inventing new names. New
  name needed only for `SURROGATE_KEY_COLUMN` (e.g. `product_sk`). `PRIMARY_KEYS` (already on
  `META_SOURCE_ENTITY`) is reused as-is for the natural/business key — no separate column needed.
- **`META_CONFIGURATION_ADVANCED`** — reserved back in ADR-0007 for "config that genuinely
  repeats" but never actually used until now — is the natural home for the fact's dimension
  lookups: one instance row per dimension the fact needs resolved (target dimension table, the
  natural-key column on the fact's own source side, the natural-key column on the dimension side,
  and the surrogate-key column to pull back). Three instances for v1 (product, customer,
  territory); a fourth for date if `OrderDate` is wired to `dimdate` in v1, otherwise deferred.

## Decision: Pipeline & Notebook Design

**Fully notebook-based**, as requested — no `Switch Execution Route` at all, since every Gold
entity takes the NOTEBOOK path by construction (mirrors Silver's reasoning in ADR-0007, one
step further).

**Two notebooks, not one**, since dimension and fact processing are genuinely different
algorithms (unlike Silver's SCD1/SCD2, which collapse into one branchable notebook):
- `NB_Gold_Dimension_Load` — generic across all sourced dimensions: hash-compare SCD2 merge
  (Silver's proven pattern) plus the new surrogate-key-assignment step.
- `NB_Gold_Fact_Load` — join the two Silver sources, resolve dimension keys via the
  `META_CONFIGURATION_ADVANCED` lookups, then the natural-key upsert `MERGE`.

**Orchestration shape — corrected against a real platform constraint.** The wave idea
("dimensions before fact, sequential; entities within a wave, parallel") cannot be built as one
`ForEach` nested inside another — `PLATFORM_CONSTRAINTS.md` is explicit that Fabric doesn't allow
nested `ForEach`. The actual mechanism, mirroring `PL_Master_Orchestrator`'s own idiom (sequential
steps at the pipeline level, each internally flat-parallel) rather than a nested loop: `PL_Gold_
Ingestion` has **two sibling `ForEach` blocks placed in sequence**, not one wrapping the other —

`Get Gold Batch (Wave 10)` → `ForEach Dimension Entities` (`isSequential=false`, `batchCount=10`)
→ *on Success* → `Get Gold Batch (Wave 20)` → `ForEach Fact Entities` (`isSequential=false`,
`batchCount=10`).

A new `spGet_Gold_Batch` stored procedure (mirroring `spGet_Bronze_Batch`/`spGet_Silver_Batch`'s
shape) takes a `@WaveNumber` parameter and returns only that wave's entities, filtered on
`ORDER_OF_OPERATIONS`. **Known limitation, worth stating plainly:** this two-Lookup-plus-two-
ForEach shape is hardcoded for exactly two waves at pipeline-design time — adding a third wave
means editing the pipeline (one more Lookup+ForEach pair), not just inserting a metadata row.
Acceptable for v1's fixed scope; would need generalizing if wave count becomes genuinely dynamic.

**Exit-value contract:** unchanged — reuses the standing
`{status, rows_read, rows_written, rows_rejected, watermark_value_used, watermark_value_new}`
contract from ADR-0005, so both new notebooks plug into the existing `Complete Log
Success`/`Failure` Script activities with zero changes there. `rows_written` for a dimension
means total upserted (brand-new + closed-out + new-version rows), matching Silver's existing
convention; for the fact, total upserted rows.

**Watermark / incremental extraction:** reuses the same watermark-column mechanism already
proven at Bronze and Silver — a `WATERMARK_COLUMN` config (category `GOLD`) pointing at Silver's
own timestamp column, so Gold only reads Silver rows that are new or changed since Gold's last
run. No new incremental mechanism invented.

**Logging/observability:** reuses `META_INGESTION_LOG` exactly as Bronze/Silver do — no new
table. It already captures per-entity, per-run status, row counts, and timestamps, which covers
the "minimal but real observability" bar for v1. A finer inserted/updated/unchanged row-count
split per dimension (beyond the single `rows_written` total) is listed in the backlog below
rather than built now.

## Consequences

- Zero new metadata tables and zero DDL changes to the metadata store — every new piece of Gold
  config rides on existing, previously mostly-unused columns/tables (`ORDER_OF_OPERATIONS`,
  `META_CONFIGURATION_ADVANCED`). This is a good sign the metadata schema was designed with more
  headroom than Bronze/Silver alone exercised.
- Adding a fifth dimension or a second fact to the *same two waves* is a pure metadata change
  (new `META_SOURCE_ENTITY`/`META_ORCHESTRATION`/`META_CONFIGURATION_CORE`/`ADVANCED` rows) — no
  pipeline or notebook change needed, as long as it fits the generic dimension or fact notebook's
  existing logic.
- Adding a genuinely new wave (e.g. a second fact that depends on the first fact's output) does
  require a pipeline edit, not just metadata — documented above rather than glossed over.
- `dimdate` is intentionally outside the recurring pipeline — a one-time generation step, not
  part of `PL_Gold_Ingestion`. Worth confirming this matches your expectation before coding
  starts, since it means "Gold all dimensions SCD2" has exactly one accepted exception.
- `INSERT_DATETIME`/`UPDATE_DATETIME`/`SOURCE_INGESTION_LOG_ID` add real observability value
  (row-level trace to the exact run that touched it) at negligible schema/notebook cost, since
  they slot into the `MERGE`'s existing insert/update branches rather than needing separate logic.

## Deferred to a future round (not built in v1)

Described generically, as capabilities a mature enterprise Gold-layer implementation
eventually needs, not tied to any specific external reference:

1. **Historically-accurate dimension key resolution** — resolving the surrogate key that was
   active *as of the transaction's own date* (a range-join against each dimension's SCD2
   start/end dates), instead of v1's "currently active row at Gold-load time" resolution.
2. **Inferred/unknown-member handling** — auto-creating a placeholder dimension row when a fact
   references a natural key not yet seen, instead of v1's quarantine-and-wait behavior.
3. **Metadata-driven multi-source joins for facts** — a general `META_CONFIGURATION_ADVANCED`-
   style declaration for facts spanning more than one Silver source table, instead of v1's
   hardcoded single join inside `NB_Gold_Fact_Load`.
4. **Dynamic wave count** — a pipeline shape that doesn't need editing every time a new
   sequencing wave is introduced.
5. **Finer Gold observability** — inserted/updated/unchanged row-count splits per dimension,
   beyond the single `rows_written` total `META_INGESTION_LOG` currently captures.
6. **Periodic snapshot fact support** — for a future point-in-time reporting need (e.g. "orders
   outstanding as of date X"), distinct from the transactional fact built here.
7. **SCD Type 3/6 hybrid tracking**, junk dimensions, aggregate/summary fact tables, and
   conformed-dimension management across multiple fact tables — all standard Kimball-methodology
   capabilities not needed until the model has more than one fact table.
8. **Full per-row source-system lineage** (`SOURCE_SYSTEM_NAME`/`SOURCE_SYSTEM_KEY`/
   `SOURCE_SYSTEM_VALUE` on every row) — deferred in favor of `SOURCE_INGESTION_LOG_ID` plus the
   already-retained natural keys (see the Row-Level Audit & Lineage Columns decision above) until
   a Gold table is genuinely fed by more than one source system, at which point per-row lineage
   stops being a static, redundant value and starts being the only way to tell rows apart.
