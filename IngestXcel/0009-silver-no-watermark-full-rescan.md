# ADR-0009: Silver No-Watermark Full-Rescan Support

## Status
Accepted, built, and proven (dbo.PersonDetails)

## Context

ADR-0007's original v1 Decision assumed every Silver entity has a real per-row watermark
column to filter its Bronze read incrementally ("Correct incremental extraction from Bronze via
the same watermark-column mechanism already proven in Bronze"). This breaks down for genuinely
keyless/timestamp-less entities — `dbo.PersonDetails` has no per-row date column at all, and
neither do file-sourced entities in general, since file-level incremental logic (ADR-0008) is
based on file modification time, not a row-level column that could flow through to Silver the
same way. There's simply no column to filter Silver's own read on for these entities.

## Decision

1. **When `WATERMARK_COLUMN` is blank for a Silver entity, the read cell skips the watermark
   filter entirely and does a full rescan of Bronze every run**, rather than trying to derive
   or fake a substitute incremental signal.
2. **This is safe because SCD1/SCD2's own change-detection was never actually
   watermark-dependent to begin with.** The hash-comparison of business columns against
   Silver's current active rows is what actually determines "changed / new / unchanged" —
   watermark only ever existed to limit *how much* of Bronze gets re-read each run, an
   efficiency concern, not a correctness one. Removing it doesn't remove any correctness
   guarantee, only some read efficiency.
3. **Dedup ordering falls back to `F.monotonically_increasing_id()`** (picking one row per
   duplicate primary key within a single batch) when no watermark exists to order by — a
   deterministic but genuinely arbitrary tie-break, not a true "latest." Named here explicitly
   as an honest limitation rather than left implicit: if the same key legitimately repeats
   within one Bronze snapshot with materially different values, which one wins is not
   meaningful, only consistent.
4. **SCD2 start/end date stamping uses a captured Python `datetime.now()`** in place of the
   watermark column's value when blank — deliberately a plain captured value, not a lazy Spark
   expression, to avoid the exact re-evaluation trap that caused the `rows_merged` lazy-recount
   bug (see `LESSONS_LEARNED.md`): a lazy expression re-evaluated after the target table has
   already been mutated would give an inconsistent, not-actually-"now" timestamp.
5. **The exit-value contract's `watermark_value_new` computation is skipped** (returns
   `watermark_value_used` unchanged) when `watermark_column` is blank — there's nothing
   meaningful to advance.

## Consequences

Proven via `dbo.PersonDetails` (SCD2, `ID` used as the entity's own confirmed key for this
specific table — explicitly not a general rule; other keyless entities may have nothing this
reliable to use). `PROCESSING_METHOD='FULL'` on such entities' Silver orchestration row is used
purely as an intent signal for a human reader — the code's actual branch condition is
`WATERMARK_COLUMN` being blank, not this value.

**Real cost, not yet tested at meaningful scale**: every run reprocesses Bronze's *entire*
table for an entity on this path, not just what changed. Fine for a small table (`PersonDetails`
is tens of rows); the cost of this approach against a genuinely large Bronze table has not been
exercised or measured.
