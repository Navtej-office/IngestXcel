# ADR-0003: Schema-per-Source-System & Connection Design

**Status:** Accepted

## Context

A single Bronze Lakehouse hosts multiple source systems. Two collision risks needed a resolution:
(1) two different source systems sharing a schema/table name, and (2) one source having two
schemas with an identically-named table.

Separately: how should the framework model connections to physical source databases — one
Fabric Connection per tech type, or something more granular?

## Decision

**Schema-per-source-system**: the Bronze schema a table lands under is resolved from the
**source** endpoint, not the target Lakehouse — stored as `BRONZE_SCHEMA_ALIAS` on the source's
`META_CONNECTION_ENDPOINT` (e.g. `FABRICTRAINING_INGESTXCEL`). This resolves collision scenario
(1). Collision scenario (2) — one source, two schemas, same table name — is handled separately by
keeping target table names schema-prefixed (e.g. `person_address` vs a hypothetical
`sales_address`), not via the schema-folder mechanism.

**Connection design**: one Fabric Connection per physical source database, not one universal
connection per tech type. Fabric's "SQL database" connector binds a Connection object to one
specific database at creation time — it does not expose separate dynamic Workspace/Database
override fields the way the Lakehouse connector does. The Connection's own GUID is stored as a
`CONNECTION_ID` property on the source endpoint (renamed from the earlier `FABRIC_CONNECTION_ID`
— not conceptually Fabric-specific once non-Fabric-native sources like Azure SQL Database also
need one), and the Copy activity's Connection field is set to "Use dynamic content" referencing
that stored ID. Adding a new physical source database requires creating one new Connection and
recording its ID in metadata — no pipeline changes.

## Consequences

Confirmed working across two source tech types (Fabric SQLDB, Azure SQL) with zero schema-name
collisions and zero pipeline changes needed when the second source system was onboarded.
