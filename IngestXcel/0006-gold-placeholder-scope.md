# ADR-0006: Gold Placeholder Scope

**Status:** Accepted — Gold not built this phase, by design

## Context

Building Gold now, before Silver even exists, would mean designing against an unproven layer.
But leaving zero trace of Gold in the metadata risks a real redesign later rather than an
additive extension.

## Decision

Gold is not built this phase. The metadata model reserves just enough shape to make a future
Gold build additive rather than a redesign:
- `'GOLD'` is a valid value wherever trigger/layer naming conventions are validated.
- `META_CONFIGURATION_CORE.CONFIGURATION_CATEGORY = 'GOLD'` is a reserved category name
  (mirroring `'BRONZE'`/`'SILVER'`), with no values seeded yet.
- No Gold stored procedures, no real `PL_Gold_Load` logic — only an empty placeholder pipeline
  shell exists.
- Standing principle, decided now to avoid re-deriving later: a Gold entity's source is Silver's
  own target for that entity — the same "layer N reads from layer N−1's output, not the original
  external system" pattern already established for Silver reading from Bronze.
- Mirroring this same placeholder principle: `'SILVER'` was likewise a reserved
  `META_CONFIGURATION_CORE`/`ADVANCED` category before Silver was built, with no values seeded
  beyond Person.Address's `SCD2_SETTINGS`. Nothing about the eventual Bronze NOTEBOOK build
  constrained Silver's design when it came time to build it — `SOURCE_QUERY_OVERRIDE` is
  Bronze/external-source-scoped only and irrelevant to Silver's Delta-to-Delta reads;
  `NB_Bronze_Staged_Ingestion` was never a notebook Silver needed to extend, Silver got its own.
  The one piece Silver had to conform to was the exit-value contract (ADR-0005).

## Consequences

When Silver was actually built, this placeholder discipline paid off exactly as intended — no
redesign was needed, only additive work. The same should hold for Gold when its time comes.
