---
title: Test Coding Guide
---


This guide describes how to write a high-quality, stable End-to-End (E2E) or Unit Test for Apache SeaTunnel.
It complements the [Coding Guide](coding-guide.md): the Coding Guide covers general PR quality, while this
guide focuses specifically on test stability and resource safety.

A good SeaTunnel test is deterministic (same result on every run), leak-free (releases every
resource it opens), and cheap (starts the fewest containers possible). The five rules below enforce that.

## 1. Use Dynamic Ports, Never Hardcode

Testcontainers assigns a random host port at startup. Hardcoding a port causes conflicts in CI, where the
port may already be taken or multiple suites run in parallel.

Resolve the host port from the container at runtime:

```java
// Bad — collides in CI
String brokerUrl = "tcp://127.0.0.1:61616";

// Good — let Testcontainers assign the host port
String brokerUrl = "tcp://" + container.getHost() + ":" + container.getMappedPort(61616);
config.setPort(container.getFirstMappedPort()); // when only one port is exposed
```

Apply the same rule to all external services:

```java
// Database
String jdbcUrl = String.format(
        "jdbc:mysql://%s:%d/%s",
        mysqlContainer.getHost(), mysqlContainer.getMappedPort(3306), DATABASE);

// HTTP service
String endpoint = String.format(
        "http://%s:%d", serviceContainer.getHost(), serviceContainer.getMappedPort(8080));
```

:::caution Job config files are the exception

The SeaTunnel job runs inside the Docker network, so its config (`.conf`) must reference the container's
network alias (`withNetworkAliases("activemq-host")`) and the internal port (e.g. `61616`), not the
mapped host port.

:::

## 2. Wait on Conditions, Never `Thread.sleep`

`Thread.sleep` is non-deterministic: too short and the test becomes flaky, too long and CI is slowed down. It
also produces no useful error when the expected state never arrives. Use
[Awaitility](https://github.com/awaitility/awaitility) to poll for the actual condition instead.

```java
// Bad — flaky, wasteful, and silent on failure
container.executeJob("/job.conf");
Thread.sleep(30000);
Assertions.assertIterableEquals(expected, query());

// Good — returns as soon as the condition holds, fails with the last assertion error
Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .pollDelay(Duration.ZERO)
        .untilAsserted(() -> Assertions.assertIterableEquals(expected, query()));
```

Choose the timeout from the scenario rather than picking a round number:

| Scenario                          | `atMost`   | `pollInterval` | Why                                  |
|-----------------------------------|------------|----------------|--------------------------------------|
| Container / client readiness      | 2 min      | 1 s            | Image pull + service init is slow    |
| Job reaches `RUNNING` state       | 1 min      | 2 s            | Scheduling overhead                  |
| Batch job result verified         | 60 s       | 2 s            | Small dataset completion             |
| Kafka / MQ message consumption    | 30–60 s    | 1 s            | Consumer group rebalance             |
| CDC / schema-change propagation   | 60–120 s   | 2–5 s          | Binlog latency + snapshot            |

When the client throws until the service is ready, add `.ignoreExceptions()`:

```java
Awaitility.given()
        .ignoreExceptions()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(180, TimeUnit.SECONDS)
        .untilAsserted(this::initProducer);
```

A reusable helper for the common "wait for job RUNNING" step:

```java
private void awaitJobRunning(TestContainer container, String jobId) {
    Awaitility.await()
            .pollInterval(2, TimeUnit.SECONDS)
            .atMost(1, TimeUnit.MINUTES)
            .untilAsserted(
                    () -> Assertions.assertEquals("RUNNING", container.getJobStatus(jobId)));
}
```

## 3. Release Resources Promptly

A leaked connection exhausts container resources and available host ports, and can cause CI to hang. Every
resource you open must be closed. Most E2E tests implement the `TestResource` interface and override its
`tearDown()` method, which runs once after all tests in the class. Close resources there in reverse order of
creation, with a null check on each (the test may have failed before the resource was created), and stop any
manually-started container explicitly.

```java
@AfterAll
@Override
public void tearDown() throws Exception {
    if (producer != null) {
        producer.close();
    }
    if (session != null) {
        session.close();
    }
    if (connection != null) {
        connection.close();
    }
    if (container != null) {
        container.stop();
    }
}
```

For resources scoped to a single method, prefer try-with-resources over manual close:

```java
private void executeDml(String sql) {
    try (Connection conn = getJdbcConnection();
            Statement stmt = conn.createStatement()) {
        stmt.execute(sql);
    } catch (SQLException e) {
        throw new RuntimeException("Execute DML failed: " + sql, e);
    }
}
```

Cleanup checklist:

- [ ] JDBC connections / statements closed
- [ ] Message broker connections, sessions, producers, consumers closed
- [ ] Custom clients (HTTP, gRPC) shut down
- [ ] `ExecutorService` shut down with a timeout
- [ ] Async `CompletableFuture` jobs cancelled
- [ ] Temporary files / directories deleted
- [ ] Manually-created Docker networks removed

## 4. Submit Long-Running Jobs Asynchronously

A streaming or CDC job runs until it is cancelled, so calling `executeJob` inline blocks the test thread
indefinitely and leaves no opportunity to inject data or assert intermediate state. Submit such jobs with
`CompletableFuture.supplyAsync`, wait until the job reaches `RUNNING`, run the test steps, verify the result,
then cancel the job.

```java
String jobId = "streaming-cdc-job";

CompletableFuture<Void> job = CompletableFuture.supplyAsync(
        () -> {
            try {
                container.executeJob("/streaming_job.conf", jobId);
            } catch (Exception e) {
                log.error("Job execution failed", e);
                throw new CompletionException(e); // propagate, never swallow
            }
            return null;
        });

awaitJobRunning(container, jobId); // wait for RUNNING before proceeding

insertCdcData();                   // run test steps while the job is running

Awaitility.await()
        .atMost(60, TimeUnit.SECONDS)
        .pollInterval(2, TimeUnit.SECONDS)
        .untilAsserted(() -> verifySinkResults());

job.cancel(true);                  // cancel the job
```

Use this table to decide:

| Job type                                   | Pattern                          |
|--------------------------------------------|----------------------------------|
| Streaming job with data injection          | `CompletableFuture.supplyAsync`  |
| Streaming CDC job                          | `CompletableFuture.supplyAsync`  |
| Dynamic partition discovery                | `CompletableFuture.supplyAsync`  |
| Batch job (source → sink that completes)   | Inline `executeJob`              |

For a batch job, assert the exit code and verify the result:

```java
Container.ExecResult result = container.executeJob("/batch_job.conf");
Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> verifyResults());
```

## 5. Keep Tests Deterministic and Cheap

- Extend `TestSuiteBase` and implement `TestResource` to gain the `startUp()` / `tearDown()` lifecycle hooks.
- Use `@TestTemplate` with a `TestContainer` parameter so the test runs across every configured engine.
- Start the fewest containers possible. Put the source and sink cases in the same class so the Docker
  image initializes once (see Coding Guide item 11).
- Cover the full data type set, not just `String` and `int`.
- Order dependent steps with `@TestMethodOrder(MethodOrderer.OrderAnnotation.class)` and `@Order(n)`.
- Add `@Slf4j` and log the key phases (data insertion, verification) to make CI failures easier to diagnose.

## 6. Whitelist Unrecyclable Client Threads (Zeta Engine)

After the last job in a test finishes, the Zeta `SeaTunnelContainer` snapshots the server JVM's live threads
and fails the test if any non-system thread is still running after a 120-second grace window. This guards
against connectors that leak threads. Most client threads die once you close the client in `tearDown()`
(Rule 3) — always try that first.

However, some third-party client libraries (JDBC drivers, HTTP clients, connection pools) start daemon
threads that are never recycled and cannot be closed from the test. These would fail the leak check through
no fault of the test. For those — and only those — whitelist the thread's **name prefix** by extending
`isIssueWeAlreadyKnow(String)` in `SeaTunnelContainer`, where the project keeps these exceptions centrally:

```java
// seatunnel-e2e/seatunnel-e2e-common/src/test/java/org/apache/seatunnel/e2e/common/
//     container/seatunnel/SeaTunnelContainer.java

/** The thread should be recycled but not, we should fix it in the future. */
protected boolean isIssueWeAlreadyKnow(String threadName) {
    return threadName.startsWith("ClickHouseClientWorker") // ClickHouse client
            || threadName.startsWith("Okio Watchdog")      // InfluxDB / OkHttp
            || threadName.startsWith("iceberg-worker-pool") // Iceberg worker pool
            // Add the new connector's thread prefix here, with a comment naming the library.
            || threadName.startsWith("<your-connector-thread-prefix>");
}
```

When adding an entry:

- Match a specific prefix, never a broad substring — a loose match can hide a real leak from another test.
- Add a comment naming the connector and the fully-qualified class that spawns the thread, following the
  existing style.
- Only whitelist threads that genuinely cannot be closed. A thread you *can* close belongs in `tearDown()`,
  not the whitelist.

:::note

Engine and JVM system threads (`hz.main`, `seatunnel-coordinator-service`, `pool-N-thread-N`, GC threads, and
so on) are already excluded by `isSystemThread(...)`. Do not duplicate them in `isIssueWeAlreadyKnow(...)`.

:::

## Putting It Together

The example below combines all five rules: a single container shared by the test methods, a dynamic port,
condition-based waiting during start-up, and resource cleanup in `tearDown()`.

```java
@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ExampleConnectorIT extends TestSuiteBase implements TestResource {

    private static final String CONTAINER_IMAGE = "example:latest";
    private static final String CONTAINER_HOST = "example-host";
    private static final int CONTAINER_PORT = 8080;

    private GenericContainer<?> container;
    private Connection connection;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        container =
                new GenericContainer<>(DockerImageName.parse(CONTAINER_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(CONTAINER_HOST)
                        .withExposedPorts(CONTAINER_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(CONTAINER_IMAGE)));
        Startables.deepStart(Stream.of(container)).join();
        log.info("Example container started");

        // Wait until the client can connect, rather than sleeping for a fixed time.
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(2, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        initializeTestData();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (container != null) {
            container.stop();
        }
    }

    @TestTemplate
    @Order(1)
    public void testBatchSourceToSink(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/example_batch.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(2, TimeUnit.SECONDS)
                .untilAsserted(this::verifyResults);
    }

    private void initConnection() {
        String endpoint =
                String.format(
                        "http://%s:%d", container.getHost(), container.getMappedPort(CONTAINER_PORT));
        connection = createConnection(endpoint);
    }
}
```

## Module and Resource Layout

A connector E2E test is its own Maven module under `seatunnel-e2e/seatunnel-connector-v2-e2e/`. The engine
loads `.conf` and other resource files from the test classpath root (`src/test/resources/`), so these
paths are conventions to follow, not arbitrary choices.

```
seatunnel-e2e/seatunnel-connector-v2-e2e/
└── connector-<name>-e2e/                                   # one module per connector
    ├── pom.xml                                             # artifactId: connector-<name>-e2e
    └── src/test/
        ├── java/org/apache/seatunnel/e2e/connector/<name>/
        │   └── <Name>IT.java                               # the IT class
        └── resources/
            ├── <name>_source_to_sink.conf                  # job configs (classpath root)
            ├── ddl/<table>.sql                             # schema / seed DDL (optional)
            ├── docker/<server>.cnf                         # container config to mount (optional)
            └── log4j2-test.properties                      # test logging (optional)
```

### Test class

- Location: `src/test/java/org/apache/seatunnel/e2e/connector/<name>/`. CDC connectors instead use the
  `org.apache.seatunnel.connectors.seatunnel.cdc.<name>` package — follow the sibling CDC modules.
- Naming: name the class `<Name>IT`. The Failsafe plugin runs `*IT` classes as integration tests, while
  Surefire runs `*Test` classes as unit tests. Use one class per behavior, e.g. `<Name>WithSchemaChangeIT`.

### Config (`.conf`) files

- Place job configs directly under `src/test/resources/`, and reference them from code with a leading
  slash: `container.executeJob("/<name>_source_to_sink.conf")`. The leading slash means classpath-absolute,
  not a filesystem path.
- Name files by intent and data flow, for example `fake_to_<sink>.conf`, `<name>_multitable.conf`, or
  `<name>cdc_to_<name>_with_schema_change.conf`. Avoid generic names such as `test.conf`.
- Inside the `.conf`, services must use the container's network alias and internal port
  (e.g. `host = "rabbitmq-e2e"`, `port = 5672`), never the mapped host port (see Rule 1).
- A `.conf` file is a source file and must carry the Apache License header as `#` comments.

### DDL / SQL and mounted resources

- Put schema and seed scripts under `src/test/resources/ddl/`, named after the database or table they build
  (e.g. `ddl/inventory.sql`). CDC helpers such as `UniqueDatabase` resolve them by the classpath name
  `ddl/<template>.sql`, so keep that exact path.
- Files mounted into a container (e.g. `my.cnf`, `setup.sql`) go under `src/test/resources/docker/` and are
  referenced through the container API (e.g. `.withSetupSQL("docker/setup.sql")`).
- Every new `.sql` file must also carry the Apache License header.

### Registering the module

A new module is not built until it is wired in:

1. Add `<module>connector-<name>-e2e</module>` to `seatunnel-e2e/seatunnel-connector-v2-e2e/pom.xml`.
2. In the module `pom.xml`, set the `artifactId` to `connector-<name>-e2e`, give it a `<name>` such as
   `SeaTunnel : E2E : Connector V2 : <Name>`, and declare test dependencies on the connector under test plus
   `connector-console`, `connector-assert`, or `connector-fake` as required by the job configs.
3. For a brand-new connector, also update the CI label and `plugin-mapping.properties` (see the
   contribution checklist).

## Verify Before Submitting

```bash
# Format
./mvnw spotless:apply

# Run the connector E2E suite (run more than once to confirm stability)
./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-<name>-e2e \
    -DskipUT -DskipIT=false verify
```

## Checklist

- [ ] No hardcoded host ports — all use `getMappedPort()` / `getFirstMappedPort()`
- [ ] No `Thread.sleep()` — all waits use Awaitility with scenario-based timeouts
- [ ] Every opened resource is closed in `@AfterAll` / `@AfterEach` or try-with-resources
- [ ] Streaming / CDC jobs submitted via `CompletableFuture.supplyAsync` and gated on `RUNNING`
- [ ] Source and sink share one container; full data types covered
- [ ] Resources follow the layout: `.conf` at the classpath root, DDL under `ddl/`, mounts under `docker/`
- [ ] New module registered in the parent `pom.xml`
- [ ] No Zeta thread-leak failure; any unrecyclable client thread whitelisted in `isIssueWeAlreadyKnow`
- [ ] Apache License header present on all new files, including `.conf` and `.sql`
