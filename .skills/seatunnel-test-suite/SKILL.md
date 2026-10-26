---
name: seatunnel-test-suite
description: Write or review Apache SeaTunnel E2E (Testcontainers) and unit tests so they are stable, leak-free, and deterministic. Use when creating a new *IT/*Test class, reviewing a test diff, or refactoring a flaky or leaking test.
---

# SeaTunnel Test Suite

Self-contained operating manual for writing and reviewing SeaTunnel tests: the rule tables, citable IDs, and
the patterns needed to act. Everything required to apply or cite a rule is in this file.

## When to use

- Writing a new connector E2E test (`*IT.java`) or unit test (`*Test.java`).
- Reviewing a test diff for flakiness, leaks, or layout violations before a PR.
- Stabilizing a flaky or leaking test, or bringing an older test up to these conventions.

## Framework contract (E2E)

Every E2E test **must** follow this structural contract — violating it means the test silently does nothing.

| Requirement | Detail |
|-------------|--------|
| Extend `TestSuiteBase` | Provides the engine container lifecycle, `NETWORK`, and test dispatch |
| `@TestInstance(Lifecycle.PER_CLASS)` | Inherited from `TestSuiteBase` — one instance shared across all methods; fields need not be `static` |
| `@TestTemplate` on each test method | NOT `@Test`. The `TestCaseInvocationContextProvider` only dispatches `@TestTemplate` methods |
| `TestContainer` parameter | Each `@TestTemplate` method must accept a single `TestContainer container` parameter |
| `@DisabledOnContainer` (optional) | Add **only** when the scenario genuinely can't run on an engine — see below. Default to no annotation so the test runs on all engines. |

Minimal skeleton — runs on every engine (Zeta, Flink, Spark), which is the default and correct case for
most batch source/sink tests:

```java
public class MyConnectorIT extends TestSuiteBase {

    @TestTemplate
    public void testSourceToSink(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/my_connector_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode());
    }
}
```

### When to add `@DisabledOnContainer`

Do not exclude an engine by default. Add the annotation only when a scenario provably cannot run on that
engine, and word `disabledReason` for the *specific* scenario — not a blanket "engine X is unsupported".
The exclusion can go on the class (whole suite) or a single `@TestTemplate` method (just that case). Real
reasons from the codebase:

| Excluded engines | Why (the real constraint) |
|------------------|---------------------------|
| Spark | CDC / streaming jobs — Spark doesn't support the continuous/changelog mode |
| Spark + Flink | checkpoint-restore tests; scenarios needing Zeta-only features |
| Spark + Flink (all but Zeta) | continuous-discovery long-running jobs; SeaTunnel-only behavior |
| Spark | drops the RowKind of a record, so changelog assertions fail |

```java
// Only this CDC case can't run on Spark — exclude at the method, not the class:
@TestTemplate
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Spark does not support the CDC streaming job")
public void testCdcStreaming(TestContainer container) throws Exception {
    // ...
}
```

## The rules

Two namespaces: **E1–E7 govern E2E (`*IT`) tests** (cite as `[E3]`); **U1–U6 govern unit (`*Test`)
tests** (cite as `[U5]`). E6 is Zeta-engine-only. Pick the namespace that matches the file under review —
never apply an E-rule to a unit test or vice versa.

| # | Rule | Check (the smell) | Fix |
|---|------|-------------------|-----|
| E1 | Dynamic ports | literal `127.0.0.1:<port>` / `localhost:<port>` in Java code | `container.getHost()` + `container.getMappedPort(p)`; `.conf` files reference the network alias + internal port (see E7) |
| E2 | Condition-based waiting | any `Thread.sleep(...)` | `Awaitility.await().atMost(...).pollInterval(...).untilAsserted(...)`; timeout from the scenario table below |
| E3 | Release resources | an opened client/connection/container with no close | implement `TestResource`, close in `tearDown()` reverse-order + null-safe; try-with-resources for method-scoped |
| E4 | Async job submission | inline `executeJob` for a streaming/CDC job (never returns) | `CompletableFuture.supplyAsync(...)`, gate on `RUNNING`, act, verify, `cancel(true)` |
| E5 | Share one container | a fresh container started per test method | share one container across all methods in a class (`@TestInstance(PER_CLASS)`) |
| E5b | Pin image versions | `:latest` image tag | pin a specific version, e.g. `mysql:8.0.32`, never `mysql:latest` (see subsection) |
| E5c | Cover all data types | only `String`/`int` exercised | cover the connector's full data-type set, not just the easy ones |
| E6 | Thread whitelist (Zeta) | `There are still threads running in the container` | close the client first (E3); if the lib thread is unrecyclable, whitelist its prefix in `isIssueWeAlreadyKnow(...)` |
| E7 | Docker network | container not reachable from engine container | `.withNetwork(NETWORK).withNetworkAliases("my-alias")`; start with `Startables.deepStart(...)`; add `Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE))` for debug logs (full setup below) |

### E7 — network setup

The SeaTunnel engine runs **inside** its own Docker container. External service containers must share the
same Docker network so the engine can reach them (this is also why E1's `.conf` files use the alias, not the
mapped host port). Setup pattern:

```java
// In your IT class @BeforeAll (NETWORK is inherited from TestSuiteBase):
container = new GenericContainer<>(DockerImageName.parse("my-service:1.2.3"))
        .withNetwork(NETWORK)                            // inherited field from TestSuiteBase
        .withNetworkAliases("my-service-host")           // reachable by this name from engine
        .withExposedPorts(3306)
        .withLogConsumer(new Slf4jLogConsumer(DockerLoggerFactory.getLogger("my-service:1.2.3")));
Startables.deepStart(Stream.of(container)).join();       // parallel startup
```

In your `.conf` file (runs inside the engine container):
```hocon
host = "my-service-host"
port = 3306
```

In your Java test setup (runs on the host):
```java
String jdbcUrl = String.format("jdbc:mysql://%s:%d/test",
        container.getHost(), container.getMappedPort(3306));
```

### E2 — timeout reference

| Scenario | atMost | pollInterval |
|----------|--------|--------------|
| Container / client readiness | 2 min | 1 s |
| Job reaches `RUNNING` | 1 min | 2 s |
| Batch job result verified | 60 s | 2 s |
| MQ / Kafka consumption | 30–60 s | 1 s |
| CDC / schema-change propagation | 60–120 s | 2–5 s |

Chain `.ignoreExceptions()` onto the `Awaitility.await()` (the E2 fix) whenever the client throws until the
service is up — otherwise the first poll's exception fails the wait instead of retrying. Never pick a bare
round number without a scenario to justify it.

### E5b — image version pinning

Never use `:latest` — upstream images can change behavior without warning, breaking CI with no code diff to
bisect. Pin to a specific minor or patch version:

```java
// Bad — breaks unpredictably
private static final String IMAGE = "postgres:latest";

// Good — reproducible
private static final String IMAGE = "postgres:14.5";
```

### E6 — thread whitelist, in one paragraph

After the last job finishes, `SeaTunnelContainer` snapshots the server JVM and fails if a non-system thread
survives 120s. Daemon threads from third-party clients (JDBC drivers, HTTP pools) are sometimes unrecyclable.
For those only, add a **specific name prefix** to `isIssueWeAlreadyKnow(String)` in `SeaTunnelContainer`, with
a comment naming the library. A thread you *can* close belongs in `tearDown()`, not the whitelist. System
threads (`hz.main`, `pool-N-thread-N`, …) are already handled by `isSystemThread(...)` — don't duplicate them.

### UT rules

Use this table for `*Test` classes (Surefire, no engine container):

| # | Rule | Smell | Fix |
|---|------|-------|-----|
| U1 | Test behavior, not internals | asserts on private fields or internal call order | assert observable output/state; verify `void` calls only when side-effect IS the contract |
| U2 | No timing or IO dependence | `Thread.sleep`, `System.currentTimeMillis()`, filesystem, network | remove timing; mock/fake IO boundaries; inject clocks if needed |
| U3 | Mock at boundaries only | mocking domain objects or `new`-able value types | only mock external interfaces/clients/maps/services the class depends on via constructor or setter |
| U4 | One behavior per method | multi-branch `if`/`for` inside a single test | split into one method per branch; name each method as the behavior it proves |
| U5 | Negative paths assert message | `assertThrows(FooException.class, ...)` with no message check | also call `assertThat(ex.getMessage()).contains("expected fragment")` |
| U6 | Naming | `testFoo`, `testBar`, `test123` | class `*Test`; methods like `shouldReturn404WhenUserMissing`, `throwsOnNullHost` |

**Mock setup shape** — each test method is Arrange-Act-Assert:
1. *Arrange* — in `@BeforeEach`, `mock(...)` every external boundary, wire the chain with `when(...).thenReturn(...)`, then construct the SUT. Per test, stub only the values that select the branch under test.
2. *Act* — call the one method under test.
3. *Assert* — check observable output (U1), not call chains.

Pitfalls that make a mocked test pass for the wrong reason:
- When the SUT reaches a collaborator through a chain (`a.getB().getC(key)`), stub the whole chain, and stub each lookup with the *exact* key constant the production code uses — a mismatched key returns `null` and silently changes the branch taken.
- To exercise a downstream branch, stub every upstream guard non-null first; a null upstream value short-circuits early, so stubbing only the downstream value tests nothing.
- For negative paths, assert the exception message fragment, not just the type (U5) — a wrong-cause throw of the same type would otherwise pass.

**When NOT to mock:**
- Simple value objects, POJOs, DTOs — use real instances.
- The class under test itself — never mock the SUT.
- When an in-memory fake (e.g. `HashMap` instead of an interface) is simpler and equally isolated.

## Layout (where things go)

One Maven module per connector under `seatunnel-e2e/seatunnel-connector-v2-e2e/connector-<name>-e2e/`. Files
load from the test **classpath root** (`src/test/resources/`):

- Test class → `src/test/java/org/apache/seatunnel/e2e/connector/<name>/<Name>IT.java`
  (CDC connectors use the `...connectors.seatunnel.cdc.<name>` package). `*IT` = integration (Failsafe),
  `*Test` = unit (Surefire).
- Job configs → `src/test/resources/<name>_source_to_sink.conf`, referenced with a **leading slash**
  (`executeJob("/<name>_source_to_sink.conf")`). License header required (`#` comments).
- DDL → `src/test/resources/ddl/<table>.sql` (CDC `UniqueDatabase` resolves `ddl/<template>.sql`).
- Container mounts → `src/test/resources/docker/...`, referenced via the container API (no leading slash).
- Register the module in the parent `pom.xml`; for a new connector also update CI label and
  `plugin-mapping.properties`.

## Review workflow

1. Grep for the obvious smells (a clean result is a hint, not a guarantee — `127\.0\.0\.1` and `localhost`
   are also matched without a port to catch host references where the port is templated separately):
   ```bash
   grep -nE "Thread\.sleep|127\.0\.0\.1|localhost|:latest" <test-file>
   ```
2. **[E1, E7]** External containers joined to `NETWORK` with aliases? `.conf` uses alias + internal port?
3. **Framework contract:** extends `TestSuiteBase`? `@TestTemplate` + `TestContainer` param (not `@Test`)?
4. **`@DisabledOnContainer`** — present only where a scenario truly can't run on an engine (no blanket exclusions), and scoped to the affected method when only one case is incompatible?
5. **[E3]** Each opened resource closed in `tearDown()` (or try-with-resources)? Container stopped explicitly?
6. **[E4]** Streaming/CDC jobs submitted via `CompletableFuture.supplyAsync` and gated on `RUNNING`?
7. **[E2]** Timeouts justified by the scenario table — no bare round numbers?
8. **[E5, E5b, E5c]** One container shared per class? Image versions pinned (no `:latest`)? Full type set?
9. Resources in the conventional paths; new module registered in the parent pom?
10. **[E6]** For Zeta E2E, run the full job set; if the thread-leak check fires, close the client first.
11. For unit tests, walk the **[U1–U6]** table; for the negative path confirm **[U5]** asserts the message.
12. Run the targeted test class multiple times to confirm stability:
    ```bash
    ./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-<name>-e2e \
        -DskipUT -DskipIT=false -Dit.test=<ITClassName> verify
    ```

## Quick-fix reference

Each row cites the rule it enforces, so a quick-fix finding maps directly to the output format below.

| Rule | Smell | Replace with |
|------|-------|--------------|
| E2 | `Thread.sleep(n)` then assert | `Awaitility.await().atMost(...).untilAsserted(...)` |
| E1 | `"tcp://localhost:61616"` | `container.getHost() + ":" + container.getMappedPort(61616)` |
| E3 | missing `tearDown()` | implement `TestResource`; null-safe, reverse-order close + `container.stop()` |
| E4 | inline `executeJob` for a streaming job | `CompletableFuture.supplyAsync(...)` + gate on `RUNNING` |
| E4 | `consumer.receive()` with no timeout | `consumer.receive(timeoutMs)` or Awaitility poll |
| E4 | swallowed exception in async block | rethrow as `CompletionException` |
| E6 | Zeta E2E fails on leftover client thread | close in `tearDown()`; if unrecyclable, whitelist in `isIssueWeAlreadyKnow` |
| — | `@Test` on E2E method | change to `@TestTemplate` + add `TestContainer container` parameter |
| E5b | `postgres:latest` | `postgres:14.5` (pin to specific version) |
| E7 | container unreachable from engine | `.withNetwork(NETWORK).withNetworkAliases("...")` |
| U5 | `assertThrows(...)` with no message check | add `assertTrue(ex.getMessage().contains("..."))` |

## Output format

When reviewing, report each finding as `[<RuleId>] <file>:<line> — <issue>` with a one-line fix, where
`<RuleId>` is an E-rule (`E1`–`E7`) for `*IT` tests or a U-rule (`U1`–`U6`) for `*Test` units — e.g.
`[E3] FooIT.java:88 — Kafka consumer never closed`. Show a before/after snippet only when the fix isn't
obvious from the rule. When writing a new class, satisfy all applicable rules and add the Apache License
header to every new file (including `.conf` and `.sql`).
