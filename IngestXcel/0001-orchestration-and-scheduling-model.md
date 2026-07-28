# ADR-0001: Orchestration and Scheduling Model

**Status:** Accepted (superseded one earlier draft — see Revision History)

## Context

Each medallion layer (Bronze, Silver, Gold), for each source system, and for each schedule
frequency, needs its own trigger name and its own set of META_ORCHESTRATION rows (see
`CONVENTIONS.md`'s `TRG_{SYSTEM_IDENTIFIER}_{LAYER}_LOAD_{FREQUENCY}` convention, e.g.
`TRG_FABRICTRAINING_INGESTXCEL_BRONZE_LOAD_DAILY`). A single trigger contains only one system's
tables at a time — it never spans multiple source systems.

An earlier draft of this decision proposed a single global master pipeline running
all-systems'-Bronze, waiting for everything, then running all-systems'-Silver. Checked against
the ISD Accelerator reference framework's own explicit guidance ("Why Broad Triggers Can Create
Unnecessary Waiting" — a fast system's Silver step waiting on an unrelated slow system's Bronze
step, for no real reason) and rejected before any code was built against it.

## Decision

- **Cross-system parallelism** is achieved by scheduling multiple independent Fabric triggers
  concurrently, each invoking `PL_Master_Orchestrator` with a different `SystemIdentifier`
  parameter — not by grouping systems within a shared trigger's execution.
- **Layer sequencing within one system** is enforced at the pipeline level: `PL_Master_
  Orchestrator` derives that system's Bronze/Silver/Gold trigger names from its `SystemIdentifier`
  and `Frequency` parameters (`@concat('TRG_', pipeline().parameters.SystemIdentifier,
  '_BRONZE_LOAD_', pipeline().parameters.Frequency)`, same pattern for Silver/Gold), and invokes
  each layer's pipeline in strict order via Invoke Pipeline (Legacy — no cross-workspace need, so
  no Connection object required, unlike Invoke Pipeline Preview) with `waitOnCompletion = true`.
  Silver never starts before Bronze completes for that system; Gold never starts before Silver
  completes.
- **Parallelism within one trigger** (across the tables belonging to one system, at one layer) is
  a single flat `ForEach` with `isSequential = false`, `batchCount = 10`, set as a **static**
  value on the activity — not a pipeline parameter (`batchCount` cannot accept dynamic content,
  see `PLATFORM_CONSTRAINTS.md`). 10 is per-system scope, comfortably covering every
  currently-registered and near-term-planned system's table count with real headroom, while
  remaining a genuine protective cap against source-connection exhaustion rather than an
  effectively-uncapped ceiling (`batchCount`'s hard platform maximum is 50; Microsoft's own
  guidance recommends starting conservative at 5–10, since real concurrency is bounded by
  connections to actual source systems, not just Fabric compute capacity). Cross-system
  parallelism never comes from this setting — it comes entirely from independent trigger fires.
  Each `ForEach` case/branch must be fully self-contained (its own logging-completion steps
  referencing only its own activity outputs directly) — no pipeline variables shared across
  iterations, unsafe under concurrent execution.
- Layer sequencing (Bronze before Silver before Gold) is justified by a **data-dependency fact,
  not a capacity-tier workaround**: each layer's watermark-based read only sees what the prior
  layer has already committed, so running them concurrently would mean the later layer either
  finds nothing new or reads a half-committed batch — true on any Fabric capacity tier, trial or
  Premium, not specific to the current dev environment.
- **Schedule frequency**: the framework supports Fabric's native scheduling/trigger feature, with
  a minimum supported frequency of once per hour, and is designed to accommodate lower-frequency
  schedules (daily, weekly, monthly) as well. See `PLATFORM_CONSTRAINTS.md` for Fabric's actual
  trigger granularity limits (finer than hourly is available; monthly needs a workaround).

## Consequences

- Adding a new source system requires zero changes to `PL_Master_Orchestrator`'s logic — just one
  new Fabric Schedule Trigger with a new `SystemIdentifier`.
- Failure isolation is clean: one system's bad run shows up in its own independent run history.
- `PL_Master_Orchestrator` is built and functionally validated end-to-end (trigger derivation,
  Bronze's full chain, Silver's full chain, correct sequencing). The one outstanding friction
  running the full chain back-to-back on trial capacity is Fabric's single-concurrent-Spark-
  session limit — confirmed not resolvable via a `Wait` buffer or High Concurrency mode (that
  feature's session-packing requires matching default Lakehouses between notebooks, which
  Bronze's and Silver's notebooks structurally can never satisfy by design). Accepted as a
  capacity-tier constraint, not a design defect — expected to resolve on Premium/paid capacity.

  - Confirmed since, not just expected: the capacity-tier constraint above is resolved in practice — subscribing an F8 pay-as-you-go capacity (~$0.18/CU-hour, pausable) and assigning it to both workspaces eliminated the single-concurrent-Spark-session limit; the full Bronze→Silver chain has since run successfully back-to-back multiple times.
  
  - Known deviation from this ADR's own Decision, not yet resolved: the LAKEHOUSE_FILE tech type's Bronze entities (Excel, CSV) currently share FabricSQLDB's existing trigger, because the SRC_INGESTXCEL_LAKEHOUSE_FILE connection endpoint reused that source's BRONZE_SCHEMA_ALIAS value rather than getting its own — directly contradicting "a single trigger... never spans multiple source systems" above. The intended fix is a dedicated trigger for LAKEHOUSE_FILE as a whole (covering every file format, since they deliberately share one connection endpoint). Not yet done: BRONZE_SCHEMA_ALIAS currently drives both the trigger's SYSTEM_IDENTIFIER and the physical Lakehouse schema name tables land under, so giving LAKEHOUSE_FILE its own trigger identity would, under the current property design, also rename its physical schema — a decision tracked here, not yet made.

## Revision History

- Draft 1: single global master pipeline (all-systems'-Bronze, then all-systems'-Silver).
  Rejected before implementation — see Context above.
- Draft 2 (current): per-system `PL_Master_Orchestrator`, as described above. This is the design
  that was actually built.
