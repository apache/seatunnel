---
name: seatunnel-test-suite
description: Write or review Apache SeaTunnel E2E (Testcontainers) and unit tests so they are stable, leak-free, and deterministic. Use when creating a new *IT/*Test class, reviewing a test diff, or refactoring flaky tests. Enforces dynamic ports, condition-based waiting (no Thread.sleep), resource cleanup, async job submission, and the Zeta thread-leak whitelist.
---

# SeaTunnel Test Suite

Operating manual for writing and reviewing SeaTunnel tests. The canonical reference — with full code
examples, the resource-layout spec, and engine-specific detail — is
`docs/en/developer/test-coding-guide.md` (`docs/zh/...` for Chinese). This skill is the checklist; read the
doc when you need an example or the exact convention.

## When to use

- Writing a new connector E2E test (`*IT.java`) or unit test (`*Test.java`).
- Reviewing a test diff for flakiness, leaks, or layout violations before a PR.
- Stabilizing a flaky or leaking test, or bringing an older test up to these conventions.

## The rules

Rules 1–5 apply to every test; rule 6 applies to Zeta-engine E2E tests.

| # | Rule | Check (the smell) | Fix |
|---|------|-------------------|-----|
| 1 | Dynamic ports | literal `127.0.0.1:<port>` / `localhost:<port>` | `container.getHost()` + `container.getMappedPort(p)` (`.conf` files keep the network alias + internal port) |
| 2 | Condition-based waiting | any `Thread.sleep(...)` | `Awaitility.await().atMost(...).pollInterval(...).untilAsserted(...)`; timeout from the scenario table below |
| 3 | Release resources | an opened client/connection/container with no close | implement `TestResource`, close in `tearDown()` reverse-order + null-safe; try-with-resources for method-scoped |
| 4 | Async job submission | inline `executeJob` for a streaming/CDC job (never returns) | `CompletableFuture.supplyAsync(...)`, gate on `RUNNING`, act, verify, `cancel(true)` |
| 5 | Deterministic & cheap | one container per case; only `String`/`int` covered | share one container across source+sink cases; cover the full data-type set |
| 6 | Thread whitelist (Zeta) | `There are still threads running in the container` | close the client first (rule 3); if the lib thread is unrecyclable, whitelist its prefix in `isIssueWeAlreadyKnow(...)` |

### Rule 2 — timeout reference

| Scenario | atMost | pollInterval |
|----------|--------|--------------|
| Container / client readiness | 2 min | 1 s |
| Job reaches `RUNNING` | 1 min | 2 s |
| Batch job result verified | 60 s | 2 s |
| MQ / Kafka consumption | 30–60 s | 1 s |
| CDC / schema-change propagation | 60–120 s | 2–5 s |

Use `.ignoreExceptions()` when the client throws until the service is up. Never pick a bare round number
without a scenario to justify it.

### Rule 6 — thread whitelist, in one paragraph

After the last job finishes, `SeaTunnelContainer` snapshots the server JVM and fails if a non-system thread
survives 120s. Daemon threads from third-party clients (JDBC drivers, HTTP pools) are sometimes unrecyclable.
For those only, add a **specific name prefix** to `isIssueWeAlreadyKnow(String)` in `SeaTunnelContainer`, with
a comment naming the library. A thread you *can* close belongs in `tearDown()`, not the whitelist. System
threads (`hz.main`, `pool-N-thread-N`, …) are already handled by `isSystemThread(...)` — don't duplicate them.

## Layout (where things go)

One Maven module per connector under `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-<name>-e2e/`. Files
load from the test **classpath root** (`src/test/resources/`), so the paths are conventions, not choices:

- Test class → `src/test/java/org/apache/seatunnel/e2e/connector/<name>/<Name>IT.java`
  (CDC connectors use the `...connectors.seatunnel.cdc.<name>` package). `*IT` = integration (Failsafe),
  `*Test` = unit (Surefire).
- Job configs → `src/test/resources/<name>_source_to_sink.conf`, referenced with a **leading slash**
  (`executeJob("/<name>_source_to_sink.conf")`). License header required (`#` comments).
- DDL → `src/test/resources/ddl/<table>.sql` (CDC `UniqueDatabase` resolves `ddl/<template>.sql`).
- Container mounts → `src/test/resources/docker/...`, referenced via the container API (no leading slash).
- Register the module in the parent `pom.xml`; for a new connector also update CI label and
  `plugin-mapping.properties`.

See the canonical doc's "Module and Resource Layout" section for the full tree and pom snippet.

## Review workflow

1. Grep for the obvious smells:
   ```bash
   grep -nE "Thread\.sleep|127\.0\.0\.1:[0-9]|localhost:[0-9]" <test-file>
   ```
2. Each opened resource closed in `tearDown()` (or try-with-resources)? Container stopped explicitly?
3. Streaming/CDC jobs submitted via `CompletableFuture.supplyAsync` and gated on `RUNNING`?
4. Timeouts justified by the scenario table — no bare round numbers?
5. Resources in the conventional paths; new module registered in the parent pom?
6. For Zeta E2E, run the full job set; if the thread-leak check fires, apply rule 6 (close first).
7. Build locally, more than once, to confirm stability:
   ```bash
   ./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-<name>-e2e \
       -DskipUT -DskipIT=false verify
   ```

## Quick-fix reference

| Smell | Replace with |
|-------|--------------|
| `Thread.sleep(n)` then assert | `Awaitility.await().atMost(...).untilAsserted(...)` |
| `"tcp://localhost:61616"` | `container.getHost() + ":" + container.getMappedPort(61616)` |
| missing `tearDown()` | implement `TestResource`; null-safe, reverse-order close + `container.stop()` |
| inline `executeJob` for a streaming job | `CompletableFuture.supplyAsync(...)` + gate on `RUNNING` |
| `consumer.receive()` with no timeout | `consumer.receive(timeoutMs)` or Awaitility poll |
| swallowed exception in async block | rethrow as `CompletionException` |
| Zeta E2E fails on leftover client thread | close in `tearDown()`; if unrecyclable, whitelist the prefix in `isIssueWeAlreadyKnow` |

## Output format

When reviewing, report each finding as `[Rule N] <file>:<line> — <issue>` with a one-line fix; show a
before/after snippet only when the fix isn't obvious from the rule. When writing a new class, satisfy all
applicable rules and add the Apache License header to every new file (including `.conf` and `.sql`).
