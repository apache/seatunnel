# STIP: Upgrade Compatibility Program (Phase 2)

**STIP Number**: TBD
**Status**: Draft
**Author**: zhang-arvin
**Created**: 2026-08-20
**Related Issues**: [#11356](https://github.com/apache/seatunnel/issues/11356), [#11239](https://github.com/apache/seatunnel/issues/11239)
**Related PRs**: [#11301](https://github.com/apache/seatunnel/pull/11301) (Phase 1)

## Abstract

Phase 1 (PR #11301) added a scheduled and manually-triggered cross-version restore workflow
with two representative scenarios. This STIP defines Phase 2: turning that initial workflow
into a **maintainable upgrade-compatibility program** with a documented compatibility contract,
expanded scenario coverage, and clear operational policies.

## Motivation

SeaTunnel has multiple stateful upgrade surfaces that can break silently across versions:

- Checkpoint / savepoint restore
- Source enumerator state restore
- Sink writer state restore
- CDC resume / restore after upgrade
- Serialized job graph and config compatibility

PR #11301 provides an important first step, but it does not yet answer the release-grade question:
**What upgrade guarantees do we actually verify before shipping changes that affect persisted runtime state?**

The remaining gaps include:

1. Scenario coverage is intentionally small (2 scenarios)
2. Not all high-risk state surfaces are represented
3. No clear policy for when upgrade checks must run on PRs
4. Failure output does not clearly identify which stage broke
5. No documented compatibility contract for the community

## Scope

### In Scope

- Define the minimum supported upgrade scenarios that must stay green on `dev`
- Expand scenario coverage for stateful source, sink, CDC, and checkpoint/restore paths
- Define trigger policy: when the workflow runs (schedule, manual, PR-gated)
- Make failures actionable: identify whether the break happened during state creation, restore, resume, or post-restore validation
- Document the compatibility contract the workflow asserts
- English and Chinese documentation updates

### Out of Scope (Phase 2)

- Exhaustive coverage of every connector and every historical version
- Turning every PR into a full compatibility matrix run
- Claiming stronger guarantees than the workflow actually verifies

## Design

### 1. Supported Version Matrix

The upgrade compatibility workflow tests **N-1 → dev** restore paths, where N-1 is the
most recent stable SeaTunnel release at the time the workflow runs.

| Component | Version Policy | Rationale |
|-----------|---------------|-----------|
| Old release (savepoint source) | Latest stable release (e.g., 2.3.13) | Represents the most common user upgrade path |
| Current build (restore target) | `dev` branch HEAD | Catches regressions before they ship |
| Future versions | Add N-2 as optional manual trigger | Lower priority; N-1 covers majority of users |

**Version sourcing**: The old release binary is downloaded from Apache mirrors
(`downloads.apache.org` / `archive.apache.org`). The workflow parameter `old_version`
defaults to the latest stable release and is updated by maintainers after each release.

### 2. Scenario Selection Strategy

Scenarios are selected based on **risk × coverage**: each scenario should exercise a
stateful surface that has historically produced regressions.

#### Phase 2 Minimum Scenario Set

| # | Scenario | State Surface Exercised | Risk Level |
|---|----------|------------------------|------------|
| 1 | `generic-fake-localfile` (existing) | Zeta checkpoint, LocalFile sink writer state | Medium |
| 2 | `mysql-cdc-multitable-localfile` (existing) | CDC source enumerator + reader state, multi-table restore | High |
| 3 | `kafka-source-localfile` | Source split enumerator state, Kafka offset restore | Medium |
| 4 | `jdbc-sink-postgres` | JDBC sink writer state, exactly-once semantics | Medium |
| 5 | `mysql-cdc-to-jdbc-sink` | Full CDC pipeline: source state + sink writer state | High |

#### Scenario Directory Structure

Each scenario lives under `tools/upgrade_compatibility/scenarios/<name>/` with:

```
<name>/
├── seatunnel.yaml          # Engine config template (uses __CHECKPOINT_DIR__)
├── job.conf                # Streaming job template (uses __SINK_DIR__)
├── assert.conf             # Batch assertion job template
├── plugin_config           # Connector artifacts for old release
├── setup.sh                # Optional: external service setup (e.g., Docker)
├── teardown.sh             # Optional: external service teardown
└── endless                 # Optional: marker for streaming (cancel-after-savepoint)
```

### 3. High-Risk State Surfaces

The following code areas are classified as **compatibility-sensitive**. Changes to these
areas should trigger the upgrade compatibility workflow:

| Surface | Modules | Examples |
|---------|---------|----------|
| Checkpoint serialization | `seatunnel-api`, `seatunnel-engine` | `CheckpointState`, `StateSerializer` |
| Source enumerator state | `seatunnel-api`, connector source modules | `SourceSplitEnumerator` state |
| Sink writer state | `seatunnel-api`, connector sink modules | `SinkWriter` state, transaction state |
| CDC offset models | CDC connector modules | `LsnOffset`, `ScnOffset`, `BinlogOffset` |
| Job graph / config | `seatunnel-api`, `seatunnel-core` | `JobConfig`, `Action`, `SeaTunnelConfig` |
| Serialization framework | `seatunnel-api` | `Serializable` contract changes |

### 4. Trigger / Gating Policy

The workflow uses a **progressive gating** model:

#### Phase 2a: Current State (already implemented)

```
schedule:        daily at 18:00 UTC (dev branch)
workflow_dispatch: manual trigger with old_version + scenario inputs
```

#### Phase 2b: PR-Selective Trigger

Add a `pull_request` trigger with **path filters** for compatibility-sensitive areas:

```yaml
on:
  pull_request:
    paths:
      - 'seatunnel-api/**'
      - 'seatunnel-engine/**'
      - 'seatunnel-core/**'
      - 'seatunnel-connectors-v2/connector-cdc-**/**'
      - 'seatunnel-translation/**'
```

This trigger is **non-blocking** initially (status check is advisory). After 4 weeks of
stable runs, the status check can be promoted to required for the filtered paths.

#### Phase 2c: Release Gate (future)

Before each release candidate, the full scenario matrix is run manually and results are
reviewed by the release manager.

### 5. Failure Classification

Failures are classified by **stage** to make output immediately actionable:

| Stage | Failure Signature | Owner |
|-------|------------------|-------|
| State creation | Old-release job fails to start or savepoint fails | Scenario author |
| Restore | Current dev fails to restore the savepoint | PR author / module owner |
| Resume | Job restores but fails to resume processing | PR author / module owner |
| Post-restore validation | Assert job fails on restored output | PR author / module owner |

The runner script already logs to stage-specific files (`old-server.log`, `old-job.log`,
`old-savepoint.log`, `current-restore.log`, `current-assert.log`). Phase 2 enhances this by:

1. Adding a **summary artifact** that clearly marks which stage failed
2. Adding **stage-level annotations** in GitHub Actions UI (`::error::` / `::warning::`)
3. Adding **diff output** between expected and actual assert results

### 6. Compatibility Contract

The workflow asserts the following compatibility guarantee:

> A savepoint created by SeaTunnel version N-1 (latest stable release) can be restored
> by the current `dev` branch build, and the job will resume processing and produce
> correct output as verified by the Assert sink.

This guarantee applies to:

- **Zeta engine** (local mode)
- **Streaming jobs** with checkpointing enabled
- **Scenarios listed in the compatibility matrix** (not all connectors)

This guarantee does NOT apply to:

- Flink / Spark engine restore paths (not yet covered)
- Connectors not represented in the scenario matrix
- Metadata-only upgrades (schema changes, config migrations)
- Savepoints from versions older than N-1 (not tested)

### 7. Artifact Sourcing and Reproducibility

| Artifact | Source | Caching |
|----------|--------|---------|
| Old release binary | Apache mirrors (downloads/archive) | Cached in `target/upgrade-compatibility/downloads/` |
| Current dev binary | Built from source via `./mvnw package -pl seatunnel-dist -am` | Not cached (always rebuilt) |
| Connector JARs (old release) | Maven Central (`dependency:get`) | Cached in `old-dist/connectors/` |
| Docker images (MySQL, Kafka, etc.) | Docker Hub | Standard Docker layer caching |

**Reproducibility rules**:

- The old release binary is identified by its SHA-256 checksum, logged at the start of each run
- Connector JARs are resolved by exact version (`<artifactId>:<version>`)
- Docker images use pinned tags (e.g., `mysql:8.0`, not `mysql:latest`)

### 8. Triaging and Ownership

| Failure Type | Triage Process | Owner |
|-------------|---------------|-------|
| Scenario-specific flake | File issue with `CI&CD` label, assign to scenario author | Scenario author |
| True compatibility regression | Bisect to offending commit, notify PR author | Compatibility workflow owner |
| Infrastructure failure | Check runner logs, re-run workflow | CI infrastructure team |

**Escalation path**: If the workflow fails on `dev` for more than 2 consecutive days,
the compatibility workflow owner must file a blocker issue and escalate to the dev mailing list.

## Implementation Plan

### Phase 2a: Documentation and Contract (this PR)

- [x] Write this STIP document (English + Chinese)
- [x] Update `tools/upgrade_compatibility/README.md` to reference the STIP
- [ ] Add compatibility contract to `docs/en/introduction/concepts/` and `docs/zh/introduction/concepts/`

### Phase 2b: Expand Scenario Coverage

- [ ] Add `kafka-source-localfile` scenario
- [ ] Add `jdbc-sink-postgres` scenario
- [ ] Add `mysql-cdc-to-jdbc-sink` scenario

### Phase 2c: PR-Selective Trigger

- [ ] Add `pull_request` trigger with path filters to the workflow
- [ ] Run in advisory mode for 4 weeks
- [ ] Promote to required status check for filtered paths

### Phase 2d: Failure Diagnostics

- [ ] Add stage-level annotations in GitHub Actions
- [ ] Add summary artifact generation
- [ ] Add assert diff output

## Alternatives Considered

### Alternative A: Full Matrix for Every PR

Running the full scenario matrix on every PR would provide maximum coverage but is
prohibitively expensive (90+ minutes per run, high runner cost). The progressive gating
model balances coverage with practicality.

### Alternative B: Only Scheduled Runs

Relying solely on scheduled runs would miss regressions introduced between runs.
The PR-selective trigger catches regressions closer to the point of introduction.

### Alternative C: Flink Engine Coverage in Phase 2

Adding Flink engine restore scenarios would be valuable but introduces significant
complexity (Flink cluster setup, different savepoint format). This is deferred to
Phase 3.

## References

- [Phase 1 PR #11301](https://github.com/apache/seatunnel/pull/11301)
- [Issue #11239](https://github.com/apache/seatunnel/issues/11239)
- [Issue #11353](https://github.com/apache/seatunnel/issues/11353) (Checkpoint timeline)
- [Issue #11354](https://github.com/apache/seatunnel/issues/11354) (CDC lag observability)
- [Issue #10177](https://github.com/apache/seatunnel/issues/10177) (Flink restore)
- [Issue #11020](https://github.com/apache/seatunnel/issues/11020) (Serialization compatibility)