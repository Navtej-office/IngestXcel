# ADR-0002: Onboarding Governance & Table Provisioning

**Status:** Accepted (not yet fully retrofitted into existing pipelines — see Consequences)

## Context

Should the framework auto-create target schemas/tables at runtime (convenient, but risky —
silent schema drift, elevated write-identity permissions), or require them to exist beforehand?

## Decision

Target schema and table(s) for all three layers (Bronze, Silver, Gold) are provisioned as a
**one-time manual setup step** during consumer onboarding/change registration — never
auto-created by any pipeline or notebook at runtime. This applies uniformly across layers;
Silver and Gold structures (SCD tracking columns, business-logic outputs) cannot be mechanically
inferred from source schema in the first place, making explicit DDL a hard requirement there,
not merely a preference carried over from Bronze.

- The framework is owned and governed by a central team. Consumers request onboarding via the
  governance forum and submit their metadata registration using the framework's specified
  template.
- As part of registration, the consumer (or the central team on their behalf) creates the schema
  and target table(s) for every layer being registered, matching exactly what's declared in
  metadata (`TARGET_ENTITY`, `BRONZE_SCHEMA_ALIAS`, and their Silver/Gold equivalents).
- All Copy/write activities use "Use existing," never auto-create. A run failing on a missing
  schema/table signals an incomplete registration, not a pipeline defect — and keeps the
  pipeline's own execution identity scoped to write/insert permissions only, with no CREATE
  TABLE/CREATE SCHEMA rights needed anywhere.
- Schema drift is not silently auto-applied at any layer. Tested directly: Fabric's Copy
  activity, writing to an existing Delta table via implicit name-based mapping, neither errors
  nor adds a genuinely new source column — it's silently dropped. This confirmed the need for an
  explicit per-entity choice (see ADR-0005) rather than relying on default Copy activity
  behavior.
- Silver/Gold schema drift: by design, changes do not need to automatically propagate beyond
  Bronze. A new source column landing in Bronze is sufficient; whether and how it flows into
  Silver/Gold is a deliberate, separate change managed through the same registration/approval
  process — not automatic.

## Consequences

**Not yet actually followed in practice.** Every Bronze/Silver table built so far
(`person_address`, `dbo.DimDate`, `person_businessentity`, both quarantine tables) was
auto-created by the notebook's first `mode("overwrite")` write, not pre-provisioned via DDL as
this decision requires — which is exactly why `person_businessentity`'s `BusinessEntityID`
accidentally picked up an inferred `NOT NULL` constraint from real data (see
`LESSONS_LEARNED.md`). The full retrofit (explicit DDL for every existing table, changing
Copy activities/notebooks to assume tables already exist) is deliberately deferred to a
dedicated end-to-end cleanup pass — Bronze and Silver Lakehouses get fully cleaned up and
tables recreated fresh via proper DDL at that point, not touched piecemeal before then.
