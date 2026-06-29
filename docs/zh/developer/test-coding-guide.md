---
title: 测试编码指南
---

# 测试编码指南

本指南介绍如何为 Apache SeaTunnel 编写一个高质量、稳定的端到端（E2E）测试或单元测试。
它是对[编码指南](coding-guide.md)的补充：编码指南覆盖通用的 PR 质量要求，而本指南专注于测试的稳定性与资源安全。

一个优秀的 SeaTunnel 测试应当是确定性的（每次运行结果一致）、无泄漏的（释放所有打开的资源）、
低成本的（启动尽可能少的容器）。

## 快速导航

- 如果是 connector 或 core 的本地逻辑校验，优先阅读 `单元测试规范`。
- 如果是依赖容器和真实链路的 source/transform/sink 联调，阅读 `E2E 测试规范`。
- 如果一个 PR 同时包含两类测试，请同时遵循两部分并在提交前分别验证。

## 单元测试规范

### 1. 测行为和契约，不测实现细节

单元测试应验证输入输出行为和配置契约，不要绑定私有方法内部实现或临时代码结构。

真实案例：`JdbcSourceFactoryTest`（模块：`connector-jdbc`）

```java
private Map<String, Object> baseConfig() {
    Map<String, Object> cfg = new HashMap<>();
    cfg.put("url", "jdbc:mysql://localhost:3306/test");
    cfg.put("driver", "com.mysql.cj.jdbc.Driver");
    return cfg;
}

@Test
void testValidConfigWithTablePath() {
    Map<String, Object> cfg = baseConfig();
    cfg.put("table_path", "test.users");
    Assertions.assertDoesNotThrow(() -> validate(cfg));
}
```

这个测试保护的是对外配置契约，即使工厂内部实现重构，测试仍然稳定。

### 2. 单元测试必须确定且本地可跑

单元测试应快速且确定：

- 不使用 `Thread.sleep`
- 不使用无固定种子的随机断言
- 不依赖墙钟时间

如果用例引入跨进程或外部依赖，请明确归类到对应测试层级，并保证断言可重复、可诊断。

### 3. 用 mock、stub、fake 隔离依赖

单元测试要把目标行为与外部 IO 隔离。对协作依赖优先使用内存实现或轻量测试替身。

每个测试方法尽量只覆盖一个断言范围，失败时能直接定位单一行为。

引擎侧真实 mock 案例：`JobInfoServiceNullSafetyTest`（模块：`seatunnel-engine-server`）

```java
@BeforeEach
void setUp() {
    nodeEngine = mock(NodeEngineImpl.class);
    hazelcastInstance = mock(HazelcastInstance.class);
    runningJobInfoMap = mock(IMap.class);
    finishedJobStateMap = mock(IMap.class);
    finishedJobMetricsMap = mock(IMap.class);
    finishedJobVertexInfoMap = mock(IMap.class);

    when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
    when(hazelcastInstance.getMap(Constant.IMAP_RUNNING_JOB_INFO)).thenReturn(runningJobInfoMap);
    when(hazelcastInstance.getMap(Constant.IMAP_FINISHED_JOB_STATE)).thenReturn(finishedJobStateMap);
    when(hazelcastInstance.getMap(Constant.IMAP_FINISHED_JOB_METRICS))
            .thenReturn(finishedJobMetricsMap);
    when(hazelcastInstance.getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO))
            .thenReturn(finishedJobVertexInfoMap);

    jobInfoService = new JobInfoService(nodeEngine);
}

@Test
void shouldReturnJobIdOnlyWhenFinishedMetricsIsMissing() {
    when(runningJobInfoMap.get(jobId)).thenReturn(null);
    when(finishedJobStateMap.get(jobId)).thenReturn(jobState);
    when(finishedJobMetricsMap.get(jobId)).thenReturn(null);

    JsonObject result = jobInfoService.getJobInfoJson(jobId);
    Assertions.assertEquals(jobId.toString(), result.getString(RestConstant.JOB_ID, null));
}
```

这个 mock 案例好的原因：

- 在边界 mock 掉外部 map 依赖，不需要启动引擎服务
- 通过 `when(...).thenReturn(...)` 精准构造单一业务分支
- 断言只关注可观察输出 JSON 契约，定位更直接

### 4. 错误路径同时校验异常类型与关键报错信息

负向用例不仅要断言异常类，还要断言关键报错内容，以保护用户可见错误质量。

真实案例：`MongodbIncrementalSourceFactoryTest`（模块：`connector-cdc-mongodb`）

```java
Assertions.assertThrows(
        MongodbConnectorException.class,
        () ->
                MongodbSourceConfigProvider.newBuilder()
                        .startupOptions(
                                new StartupConfig(StartupMode.EARLIEST, null, null, null)));
```

### 5. 命名清晰，结构采用 Arrange-Act-Assert

- 类名使用 `*Test`，方法名明确表达行为或错误场景。
- 每个测试按 Arrange-Act-Assert 组织。
- 避免在测试方法里写大量准备逻辑，可提取成小型复用 helper。

## 单元测试检查清单

- [ ] 类名符合 `*Test`，方法名可直接表达行为或错误场景
- [ ] 单元测试保持确定性执行，避免不必要的运行时依赖
- [ ] 断言目标是行为和契约，而非实现细节
- [ ] 负向用例同时断言异常类型和关键报错信息
- [ ] 测试数据准备最小且可复用

## E2E 测试规范

## 1. 使用动态端口，禁止硬编码

Testcontainers 在启动时会分配一个随机的宿主机端口。硬编码端口会在 CI 中引发冲突——端口可能已被占用，
或者多个测试套件并行运行。

应在运行时从容器解析宿主机端口：

```java
// Bad — collides in CI
String brokerUrl = "tcp://127.0.0.1:61616";

// Good — let Testcontainers assign the host port
String brokerUrl = "tcp://" + container.getHost() + ":" + container.getMappedPort(61616);
config.setPort(container.getFirstMappedPort()); // when only one port is exposed
```

对所有外部服务都适用此规则：

```java
// Database
String jdbcUrl = String.format(
        "jdbc:mysql://%s:%d/%s",
        mysqlContainer.getHost(), mysqlContainer.getMappedPort(3306), DATABASE);

// HTTP service
String endpoint = String.format(
        "http://%s:%d", serviceContainer.getHost(), serviceContainer.getMappedPort(8080));
```

:::caution 作业配置文件是个例外

SeaTunnel 作业运行在 Docker 网络内部，因此其配置（`.conf`）必须引用容器的网络别名
（`withNetworkAliases("activemq-host")`）和内部端口（如 `61616`），而不是映射后的宿主机端口。

:::

## 2. 基于条件等待，禁止 `Thread.sleep`

`Thread.sleep` 是非确定性的：时间太短测试会不稳定，太长则拖慢 CI。当期望状态始终未到达时，它也无法给出有用的
错误信息。应使用 [Awaitility](https://github.com/awaitility/awaitility) 轮询真实条件。

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

应根据场景选择超时时间，而非随手填一个整数：

| 场景                        | `atMost`   | `pollInterval` | 原因                       |
|---------------------------|------------|----------------|--------------------------|
| 容器 / 客户端就绪              | 2 分钟      | 1 秒           | 镜像拉取 + 服务初始化较慢       |
| 作业进入 `RUNNING` 状态       | 1 分钟      | 2 秒           | 调度开销                    |
| 批作业结果校验               | 60 秒      | 2 秒           | 小数据集完成                 |
| Kafka / MQ 消息消费          | 30–60 秒   | 1 秒           | 消费组再平衡                 |
| CDC / Schema 变更传播        | 60–120 秒  | 2–5 秒         | binlog 延迟 + 快照          |

当客户端在服务就绪前会抛异常时，加上 `.ignoreExceptions()`：

```java
Awaitility.given()
        .ignoreExceptions()
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .atMost(180, TimeUnit.SECONDS)
        .untilAsserted(this::initProducer);
```

一个可复用的「等待作业 RUNNING」辅助方法：

```java
private void awaitJobRunning(TestContainer container, String jobId) {
    Awaitility.await()
            .pollInterval(2, TimeUnit.SECONDS)
            .atMost(1, TimeUnit.MINUTES)
            .untilAsserted(
                    () -> Assertions.assertEquals("RUNNING", container.getJobStatus(jobId)));
}
```

## 3. 及时释放资源

泄漏的连接会耗尽容器资源和宿主机可用端口，并可能导致 CI 挂起。你打开的每一个资源都必须关闭。大多数 E2E
测试会实现 `TestResource` 接口并重写其 `tearDown()` 方法，该方法在类中所有测试结束后执行一次。应在其中按
创建的逆序关闭资源，并对每个资源做 null 检查（测试可能在资源创建前就已失败），同时显式停止手动启动的容器。

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

对于仅在单个方法内使用的资源，优先使用 try-with-resources 而非手动 close：

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

资源清理检查清单：

- [ ] JDBC 连接 / Statement 已关闭
- [ ] 消息中间件的 connection、session、producer、consumer 已关闭
- [ ] 自定义客户端（HTTP、gRPC）已关闭
- [ ] `ExecutorService` 带超时关闭
- [ ] 异步的 `CompletableFuture` 作业已取消
- [ ] 临时文件 / 目录已删除
- [ ] 手动创建的 Docker 网络已移除

## 4. 异步提交长时间运行的作业

流作业或 CDC 作业会一直运行直到被取消，因此内联调用 `executeJob` 会使测试线程无限期阻塞，也就没有机会注入
数据或断言中间状态。这类作业应使用 `CompletableFuture.supplyAsync` 提交，等待作业进入 `RUNNING` 状态，执行
测试步骤，校验结果，最后取消作业。

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

用下表判断：

| 作业类型                          | 模式                              |
|---------------------------------|----------------------------------|
| 带数据注入的流作业                  | `CompletableFuture.supplyAsync`  |
| 流式 CDC 作业                     | `CompletableFuture.supplyAsync`  |
| 动态分区发现                      | `CompletableFuture.supplyAsync`  |
| 批作业（source → sink 会自然结束）  | 内联 `executeJob`                 |

对于批作业，断言退出码并校验结果：

```java
Container.ExecResult result = container.executeJob("/batch_job.conf");
Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
Awaitility.await().atMost(30, TimeUnit.SECONDS).untilAsserted(() -> verifyResults());
```

## 5. 保持测试确定且低成本

- 继承 `TestSuiteBase` 并实现 `TestResource` 接口，以获得 `startUp()` / `tearDown()` 生命周期钩子。
- 使用带 `TestContainer` 参数的 `@TestTemplate`，使测试在每个已配置的引擎上运行。
- 启动尽可能少的容器。把 source 和 sink 的用例放在同一个类中，让 Docker 镜像只初始化一次
  （参见编码指南第 11 条）。
- 覆盖完整的数据类型集合，不要只测 `String` 和 `int`。
- 用 `@TestMethodOrder(MethodOrderer.OrderAnnotation.class)` 和 `@Order(n)` 对有依赖的步骤排序。
- 加上 `@Slf4j` 并记录关键阶段（数据插入、校验）的日志，便于排查 CI 失败。

## 6. 将无法回收的客户端线程加入白名单（Zeta 引擎）

测试中最后一个作业结束后，Zeta 的 `SeaTunnelContainer` 会对服务端 JVM 的存活线程做一次快照，如果在 120 秒
宽限期之后仍有非系统线程在运行，就会让测试失败。这是为了防止 connector 泄漏线程。大多数客户端线程在你于
`tearDown()` 中关闭客户端后（规则 3）就会消亡——请务必先尝试这种方式。

但有些第三方客户端库（JDBC 驱动、HTTP 客户端、连接池）会启动永远不会被回收、且测试无法关闭的守护线程。这类
线程会让泄漏检查失败，而这并非测试本身的问题。仅针对这种情况，可以通过扩展 `SeaTunnelContainer` 中的
`isIssueWeAlreadyKnow(String)` 方法，把该线程的名称前缀加入白名单——项目将这些例外集中维护于此：

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

添加白名单条目时：

- 匹配具体的前缀，绝不要用宽泛的子串——过松的匹配会掩盖其他测试的真实泄漏。
- 按现有风格加上注释，说明所属 connector 以及启动该线程的全限定类名。
- 只对确实无法关闭的线程加白名单。能够关闭的线程应放在 `tearDown()` 中处理，而不是白名单。

:::note

引擎与 JVM 系统线程（`hz.main`、`seatunnel-coordinator-service`、`pool-N-thread-N`、GC 线程等）已由
`isSystemThread(...)` 排除，请勿在 `isIssueWeAlreadyKnow(...)` 中重复添加。

:::

## 整体示例

下面的示例综合了全部五条规则：测试方法共用一个容器、使用动态端口、在启动阶段基于条件等待，并在
`tearDown()` 中清理资源。

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

## 模块与资源结构

一个 connector 的 E2E 测试是位于 `seatunnel-e2e/seatunnel-connector-v2-e2e/` 下的独立 Maven 模块。引擎从测试的
classpath 根目录（`src/test/resources/`）加载 `.conf` 及其他资源文件，因此这些路径是必须遵循的约定，而非随意
摆放。

```
seatunnel-e2e/seatunnel-connector-v2-e2e/
└── connector-<name>-e2e/                                   # 每个 connector 一个模块
    ├── pom.xml                                             # artifactId: connector-<name>-e2e
    └── src/test/
        ├── java/org/apache/seatunnel/e2e/connector/<name>/
        │   └── <Name>IT.java                               # IT 测试类
        └── resources/
            ├── <name>_source_to_sink.conf                  # 作业配置（classpath 根目录）
            ├── ddl/<table>.sql                             # 建表 / 初始化 DDL（可选）
            ├── docker/<server>.cnf                         # 挂载到容器的配置（可选）
            └── log4j2-test.properties                      # 测试日志（可选）
```

### 测试类

- 位置： `src/test/java/org/apache/seatunnel/e2e/connector/<name>/`。CDC connector 改用
  `org.apache.seatunnel.connectors.seatunnel.cdc.<name>` 包——请与同级的 CDC 模块保持一致。
- 命名： 测试类命名为 `<Name>IT`。Failsafe 插件以集成测试方式运行 `*IT` 类，而 Surefire 以单元测试方式运行
  `*Test` 类。每个行为一个类，例如 `<Name>WithSchemaChangeIT`。

### 配置（`.conf`）文件

- 作业配置直接放在 `src/test/resources/` 下，并在代码中用前导斜线引用：
  `container.executeJob("/<name>_source_to_sink.conf")`。前导斜线表示 classpath 绝对路径，而非文件系统路径。
- 按用途与数据流向命名，例如 `fake_to_<sink>.conf`、`<name>_multitable.conf` 或
  `<name>cdc_to_<name>_with_schema_change.conf`。避免使用 `test.conf` 这类泛化名称。
- 在 `.conf` 内部，服务必须使用容器的网络别名和内部端口（如 `host = "rabbitmq-e2e"`、`port = 5672`），
  而不是映射后的宿主机端口（参见规则 1）。
- `.conf` 文件属于源文件，必须以 `#` 注释形式带上 Apache License 头。

### DDL / SQL 及挂载资源

- 建表与初始化脚本放在 `src/test/resources/ddl/` 下，按其所建的数据库或表命名（如 `ddl/inventory.sql`）。
  `UniqueDatabase` 等 CDC 辅助类按 classpath 名称 `ddl/<template>.sql` 解析，请保持该路径不变。
- 挂载到容器的文件（如 `my.cnf`、`setup.sql`）放在 `src/test/resources/docker/` 下，通过容器 API 引用
  （如 `.withSetupSQL("docker/setup.sql")`）。
- 每个新增的 `.sql` 文件同样必须带上 Apache License 头。

### 注册模块

新模块在被纳入构建前不会被编译：

1. 在 `seatunnel-e2e/seatunnel-connector-v2-e2e/pom.xml` 中加入 `<module>connector-<name>-e2e</module>`。
2. 在模块的 `pom.xml` 中将 `artifactId` 设为 `connector-<name>-e2e`，给定形如
   `SeaTunnel : E2E : Connector V2 : <Name>` 的 `<name>`，并按作业配置所需声明对被测 connector 以及
   `connector-console`、`connector-assert`、`connector-fake` 的测试依赖。
3. 对于全新的 connector，还需更新 CI label 和 `plugin-mapping.properties`（参见贡献检查清单）。

## 提交前验证

```bash
# Format
./mvnw spotless:apply

# Run a specific E2E test class (run more than once to confirm stability)
./mvnw -pl seatunnel-e2e/seatunnel-connector-v2-e2e/connector-<name>-e2e \
    -DskipUT -DskipIT=false -Dit.test=<TestClassName> verify
```

## 检查清单

- [ ] UT 遵循 `*Test` 命名、确定性执行和依赖隔离
- [ ] UT 负向用例同时断言异常类型和关键报错信息
- [ ] 无硬编码宿主机端口——全部使用 `getMappedPort()` / `getFirstMappedPort()`
- [ ] 无 `Thread.sleep()`——所有等待都使用 Awaitility，并按场景设定超时
- [ ] 每个打开的资源都在 `@AfterAll` / `@AfterEach` 或 try-with-resources 中关闭
- [ ] 流 / CDC 作业通过 `CompletableFuture.supplyAsync` 提交，并以 `RUNNING` 为前置条件
- [ ] source 与 sink 共用一个容器；覆盖完整数据类型
- [ ] 资源遵循约定结构：`.conf` 位于 classpath 根目录，DDL 在 `ddl/` 下，挂载文件在 `docker/` 下
- [ ] 新模块已在父 `pom.xml` 中注册
- [ ] 无 Zeta 线程泄漏失败；无法回收的客户端线程已在 `isIssueWeAlreadyKnow` 中加入白名单
- [ ] 所有新文件（含 `.conf` 和 `.sql`）都带有 Apache License 头
