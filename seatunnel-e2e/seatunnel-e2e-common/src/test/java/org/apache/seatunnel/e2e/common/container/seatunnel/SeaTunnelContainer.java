/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.common.container.seatunnel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.MavenJarUtil;

import org.apache.commons.compress.utils.Lists;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.google.auto.service.AutoService;
import groovy.lang.Tuple2;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.adaptPathForWin;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.copyAllConnectorJarToContainer;

@NoArgsConstructor
@Slf4j
@AutoService(TestContainer.class)
public class SeaTunnelContainer extends AbstractTestContainer {
    public static final String SERVER_JVM_OPTION_PROPERTY =
            "seatunnel.e2e.seatunnel.server.jvm.option";
    public static final String CLIENT_JVM_OPTION_PROPERTY =
            "seatunnel.e2e.seatunnel.client.jvm.option";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String REST_STOP_JOB_PATH = "/stop-job";
    private static final String REST_CHECKPOINT_OVERVIEW_PATH = "/jobs/checkpoints";
    protected static final String JDK_DOCKER_IMAGE = "seatunnelhub/openjdk:8u342";
    private static final String CLIENT_SHELL = "seatunnel.sh";
    protected static final String SERVER_SHELL = "seatunnel-cluster.sh";
    protected static final String CONNECTOR_CHECK_SHELL = "seatunnel-connector.sh";
    protected GenericContainer<?> server;
    private final AtomicInteger runningCount = new AtomicInteger();

    @Override
    public void startUp() throws Exception {
        FileUtils.createNewDir(HOST_VOLUME_MOUNT_PATH);
        server = createSeaTunnelServer();
    }

    /**
     * Start up the seatunnel server with the given network.
     *
     * @param NETWORK the network to use
     */
    public void startUp(Network NETWORK) throws Exception {
        server = createSeaTunnelServer(NETWORK);
    }

    private GenericContainer<?> createSeaTunnelServer() throws IOException, InterruptedException {
        return createSeaTunnelServer(NETWORK);
    }

    private GenericContainer<?> createSeaTunnelServer(Network NETWORK)
            throws IOException, InterruptedException {
        GenericContainer<?> server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(buildStartCommand())
                        .withNetworkAliases("server")
                        .withExposedPorts(5801, 8080)
                        .withFileSystemBind("/tmp", "/opt/hive")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .withFileSystemBind(
                                HOST_VOLUME_MOUNT_PATH,
                                CONTAINER_VOLUME_MOUNT_PATH,
                                BindMode.READ_WRITE)
                        .waitingFor(Wait.forLogMessage(".*received new worker register:.*", 1));
        copySeaTunnelStarterToContainer(server);
        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(MavenJarUtil.getHadoop3UberJarPath()),
                CONTAINER_HADOOP_JAR_PATH.toString());
        // execute extra commands
        executeExtraCommands(server);

        server.start();

        return server;
    }

    protected String[] buildStartCommand() {
        List<String> command = new ArrayList<>();
        command.add(
                ContainerUtil.adaptPathForWin(
                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()));
        String serverJvmOption = System.getProperty(SERVER_JVM_OPTION_PROPERTY);
        if (!isBlank(serverJvmOption)) {
            command.add("-DJvmOption=" + serverJvmOption);
        }
        return command.toArray(new String[0]);
    }

    protected GenericContainer<?> createSeaTunnelContainerWithFakeSourceAndInMemorySink(
            String configFilePath) throws IOException, InterruptedException {
        GenericContainer<?> server =
                new GenericContainer<>(getDockerImage())
                        .withNetwork(NETWORK)
                        .withEnv("TZ", "UTC")
                        .withCommand(
                                ContainerUtil.adaptPathForWin(
                                        Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString()))
                        .withNetworkAliases("server")
                        .withExposedPorts(5801, 8080)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                "seatunnel-engine:" + JDK_DOCKER_IMAGE)))
                        .waitingFor(Wait.forLogMessage(".*received new worker register:.*", 1));
        copySeaTunnelStarterToContainer(server);

        server.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/"),
                Paths.get(SEATUNNEL_HOME, "config").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(configFilePath),
                Paths.get(SEATUNNEL_HOME, "config", "seatunnel.yaml").toString());

        server.withCopyFileToContainer(
                MountableFile.forHostPath(MavenJarUtil.getHadoop3UberJarPath()),
                CONTAINER_HADOOP_JAR_PATH.toString());

        server.start();
        // execute extra commands
        executeExtraCommands(server);

        File module = new File(PROJECT_ROOT_PATH + File.separator + getConnectorModulePath());
        List<File> connectorFiles =
                ContainerUtil.getConnectorFiles(
                        module, Collections.singleton("connector-fake"), getConnectorNamePrefix());
        URL url =
                FileUtils.searchJarFiles(
                                Paths.get(
                                        PROJECT_ROOT_PATH
                                                + File.separator
                                                + "seatunnel-e2e/seatunnel-e2e-common/target"))
                        .stream()
                        .filter(jar -> jar.toString().endsWith("-tests.jar"))
                        .findFirst()
                        .get();
        connectorFiles.add(new File(url.getFile()));
        connectorFiles.forEach(
                jar ->
                        server.copyFileToContainer(
                                MountableFile.forHostPath(jar.getAbsolutePath()),
                                Paths.get(SEATUNNEL_HOME, "connectors", jar.getName()).toString()));
        server.copyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/fake-and-inmemory/plugin-mapping.properties"),
                Paths.get(SEATUNNEL_HOME, "connectors", "plugin-mapping.properties").toString());
        return server;
    }

    @Override
    public void tearDown() throws Exception {
        if (server != null) {
            // delete the volume
            server.execInContainer("rm", "-rf", CONTAINER_VOLUME_MOUNT_PATH);
            server.close();
        }
        FileUtils.deleteFile(HOST_VOLUME_MOUNT_PATH);
    }

    @Override
    protected String getDockerImage() {
        return JDK_DOCKER_IMAGE;
    }

    @Override
    protected String getStartModuleName() {
        return "seatunnel-starter";
    }

    @Override
    protected String getStartShellName() {
        return CLIENT_SHELL;
    }

    @Override
    protected String getConnectorModulePath() {
        return "seatunnel-connectors-v2";
    }

    @Override
    protected String getConnectorType() {
        return "seatunnel";
    }

    @Override
    protected String getConnectorNamePrefix() {
        return "connector-";
    }

    @Override
    protected List<String> getExtraStartShellCommands() {
        String clientJvmOption = System.getProperty(CLIENT_JVM_OPTION_PROPERTY);
        if (isBlank(clientJvmOption)) {
            return Collections.emptyList();
        }
        return Collections.singletonList("-DJvmOption=" + clientJvmOption);
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    @Override
    public TestContainerId identifier() {
        return TestContainerId.SEATUNNEL;
    }

    @Override
    protected String getSavePointCommand() {
        return "-s";
    }

    @Override
    protected String getCancelJobCommand() {
        return "-can";
    }

    @Override
    protected String getRestoreCommand() {
        return "-r";
    }

    @Override
    public void executeExtraCommands(ContainerExtendedFactory extendedFactory)
            throws IOException, InterruptedException {
        extendedFactory.extend(server);
    }

    @Override
    public Container.ExecResult executeConnectorCheck(String[] args)
            throws IOException, InterruptedException {
        // copy all connectors
        copyAllConnectorJarToContainer(
                server,
                getConnectorModulePath(),
                getConnectorNamePrefix(),
                getConnectorType(),
                SEATUNNEL_HOME);
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", CONNECTOR_CHECK_SHELL).toString();
        command.add(adaptPathForWin(binPath));
        Arrays.stream(args).forEach(arg -> command.add(arg));
        return executeCommand(server, command);
    }

    public Container.ExecResult executeBaseCommand(String[] args)
            throws IOException, InterruptedException {
        final List<String> command = new ArrayList<>();
        String binPath = Paths.get(SEATUNNEL_HOME, "bin", getStartShellName()).toString();
        command.add(adaptPathForWin(binPath));
        Arrays.stream(args).forEach(arg -> command.add(arg));
        return executeCommand(server, command);
    }

    @Override
    public Container.ExecResult executeJob(String confFile)
            throws IOException, InterruptedException {
        return executeJob(confFile, Lists.newArrayList());
    }

    @Override
    public Container.ExecResult executeJob(String confFile, List<String> variables)
            throws IOException, InterruptedException {
        return doExecuteJob(confFile, null, variables);
    }

    @Override
    public Container.ExecResult executeJob(String confFile, String jobId, String... variables)
            throws IOException, InterruptedException {
        return doExecuteJob(confFile, jobId, variables != null ? Arrays.asList(variables) : null);
    }

    private Container.ExecResult doExecuteJob(String confFile, String jobId, List<String> variables)
            throws IOException, InterruptedException {
        log.info("test in container: {}", identifier());
        List<String> beforeThreads = ContainerUtil.getJVMThreadNames(server);
        runningCount.incrementAndGet();
        Container.ExecResult result = executeJob(server, confFile, jobId, variables);
        if (runningCount.decrementAndGet() > 0) {
            // only check thread when job all finished.
            return result;
        }
        List<String> afterThreads = ContainerUtil.getJVMThreadNames(server);
        afterThreads = removeSystemThread(beforeThreads, afterThreads);
        if (afterThreads.isEmpty()) {
            //            classLoaderObjectCheck(1);
            return result;
        } else {
            // Waiting 120s for release thread
            Awaitility.await()
                    .atMost(120, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                List<String> threads = ContainerUtil.getJVMThreadNames(server);
                                threads = removeSystemThread(beforeThreads, threads);
                                List<String> finalAfterThreads = threads;
                                Assertions.assertTrue(
                                        threads.isEmpty(),
                                        "There are still threads running in the container: \n"
                                                + ContainerUtil.getJVMThreads(server).stream()
                                                        .filter(
                                                                tuple2 ->
                                                                        finalAfterThreads.contains(
                                                                                tuple2.getV1()))
                                                        .map(Tuple2::getV2)
                                                        .map(str -> str + "\n")
                                                        .collect(Collectors.joining()));
                            });
        }
        return result;
    }

    private List<String> removeSystemThread(List<String> beforeThreads, List<String> afterThreads)
            throws IOException {
        afterThreads.removeIf(SeaTunnelContainer::isSystemThread);
        afterThreads.removeIf(beforeThreads::contains);
        Map<String, String> threadAndClassLoader = getThreadClassLoader();
        List<String> notSystemClassLoaderThread =
                threadAndClassLoader.entrySet().stream()
                        .filter(
                                tc -> {
                                    // system thread, ttl 60s
                                    if (tc.getKey().contains("process reaper")) {
                                        return false;
                                    }
                                    String classLoader = tc.getValue();
                                    return !classLoader.contains("AppClassLoader")
                                            && !classLoader.equals("null");
                                })
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        notSystemClassLoaderThread.addAll(afterThreads);
        notSystemClassLoaderThread.removeIf(this::isIssueWeAlreadyKnow);
        notSystemClassLoaderThread.removeIf(SeaTunnelContainer::isSystemThread);
        return notSystemClassLoaderThread;
    }

    private static boolean isSystemThread(String s) {
        Pattern aqsThread = Pattern.compile("pool-[0-9]-thread-[0-9]");
        return s.startsWith("hz.main")
                || s.startsWith("seatunnel-coordinator-service")
                || s.startsWith("seatunnel-metrics-fetch-")
                || s.startsWith("pending-job-schedule-runner")
                || s.startsWith("GC task thread")
                || s.contains("CompilerThread")
                || s.startsWith("SeaTunnel-CompletableFuture-Thread-")
                || s.contains("NioNetworking-closeListenerExecutor")
                || s.contains("ForkJoinPool.commonPool")
                || s.contains("DestroyJavaVM")
                || s.contains("main-query-state-checker")
                || s.contains("Keep-Alive-SocketCleaner")
                // SeaTunnel REST service thread, owned by the test container lifecycle.
                || s.startsWith("Connector-Scheduler-")
                || s.contains("process reaper")
                || s.startsWith("Timer-")
                || s.contains("InterruptTimer")
                || s.contains("Java2D Disposer")
                || s.contains("OkHttp ConnectionPool")
                || s.startsWith("http-report-event-scheduler")
                || s.startsWith("event-forwarder")
                || s.contains(
                        "org.apache.hadoop.fs.FileSystem$Statistics$StatisticsDataReferenceCleaner")
                || s.startsWith("Log4j2-TF-")
                || s.startsWith("heartbeat") // Add heartbeat threads as system threads
                || aqsThread.matcher(s).matches()
                // The renewed background thread of the hdfs client
                || s.startsWith("LeaseRenewer")
                // The read of hdfs which has the thread that is all in running status
                || s.startsWith("org.apache.hadoop.hdfs.PeerCache")
                || s.startsWith("java-sdk-progress-listener-callback-thread")
                // redis pool evictor daemon thread
                || s.startsWith("commons-pool-evictor")
                // MySQL JDBC driver abandoned connection cleanup thread
                || s.startsWith("mysql-cj-abandoned-connection-cleanup")
                // Error sink worker threads
                || s.startsWith("seatunnel-error-sink-")
                // Jetty QueuedThreadPool NIO selector thread from the embedded REST server;
                // it may outlive the job and cause the E2E thread-leak check to fail.
                || s.startsWith("qtp");
    }

    private void classLoaderObjectCheck(Integer maxSize) throws IOException, InterruptedException {
        Map<String, Integer> objects = ContainerUtil.getJVMLiveObject(server);
        String className =
                "org.apache.seatunnel.engine.common.loader.SeaTunnelChildFirstClassLoader";
        if (objects.containsKey(className) && objects.get(className) > maxSize) {
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Map<String, Integer> newObjects =
                                        ContainerUtil.getJVMLiveObject(server);
                                if (newObjects.containsKey(className)) {
                                    Assertions.assertTrue(
                                            newObjects.get(className) <= maxSize,
                                            "There are still SeaTunnelChildFirstClassLoader objects in the seatunnel server");
                                }
                            });
        }
    }

    private Map<String, String> getThreadClassLoader() throws IOException {
        // Resolve the mapped REST endpoint at runtime so E2E clusters do not collide and remote
        // Docker hosts remain reachable.
        HttpGet get =
                new HttpGet(
                        String.format(
                                "http://%s:%s/hazelcast/rest/maps/running-threads",
                                server.getHost(), server.getMappedPort(5801)));
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            CloseableHttpResponse response = client.execute(get);
            String threads = EntityUtils.toString(response.getEntity());
            List<Map<String, String>> value =
                    OBJECT_MAPPER.readValue(
                            threads, new TypeReference<List<Map<String, String>>>() {});
            return value.stream()
                    .collect(
                            Collectors.toMap(
                                    map -> map.get("threadName"),
                                    map -> map.get("classLoader"),
                                    (a, b) -> a + " && " + b));
        }
    }

    /**
     * Enables the {@code parallel-\d+} thread-name exemption for the duration of the Couchbase E2E
     * test.
     *
     * <p>Must be called from {@code CouchbaseIT.startUp()} (the {@code @BeforeAll} hook) so that
     * the exemption is active only while the Couchbase test lifecycle is running, not for the
     * entire lifetime of any JVM that happens to have the Couchbase SDK on its classpath.
     *
     * @see #disableCouchbaseParallelThreadExemption()
     */
    public static void enableCouchbaseParallelThreadExemption() {
        couchbaseE2eActive = true;
    }

    /**
     * Disables the {@code parallel-\d+} thread-name exemption after the Couchbase E2E test
     * completes.
     *
     * <p>Must be called from {@code CouchbaseIT.tearDown()} (the {@code @AfterAll} hook).
     *
     * @see #enableCouchbaseParallelThreadExemption()
     */
    public static void disableCouchbaseParallelThreadExemption() {
        couchbaseE2eActive = false;
    }

    /** Enables Reactor thread exemptions while the Azure Queue Storage E2E test is active. */
    public static void enableAzureQueueReactorThreadExemption() {
        azureQueueE2eActive = true;
    }

    /** Disables Reactor thread exemptions after the Azure Queue Storage E2E test completes. */
    public static void disableAzureQueueReactorThreadExemption() {
        azureQueueE2eActive = false;
    }

    /** Enables GCS OpenCensus thread exemptions while the GCS file E2E test is active. */
    public static void enableGcsOpenCensusThreadExemption() {
        gcsE2eActive = true;
    }

    /** Disables GCS OpenCensus thread exemptions after the GCS file E2E test completes. */
    public static void disableGcsOpenCensusThreadExemption() {
        gcsE2eActive = false;
    }

    /**
     * {@code true} while the Couchbase E2E test ({@code CouchbaseIT}) is active.
     *
     * <p>Set by {@link #enableCouchbaseParallelThreadExemption()} in {@code @BeforeAll} and cleared
     * by {@link #disableCouchbaseParallelThreadExemption()} in {@code @AfterAll}. Scoping the flag
     * to the test lifecycle — rather than checking classpath availability — ensures that the {@code
     * parallel-\d+} exemption cannot silently swallow Reactor thread leaks from unrelated
     * connectors running in the same JVM.
     */
    static volatile boolean couchbaseE2eActive = false;

    /** {@code true} while the Azure Queue Storage E2E test is active. */
    static volatile boolean azureQueueE2eActive = false;

    /** {@code true} while the GCS file E2E test is active. */
    static volatile boolean gcsE2eActive = false;

    /** The thread should be recycled but not, we should fix it in the future. */
    protected boolean isIssueWeAlreadyKnow(String threadName) {
        // Couchbase SDK JVM-global static singleton threads.
        //
        // SimplePauseDetectorThread  – GC-pause latency detector (cb-core)
        // dnsjava NIO selector       – DNS resolution I/O loop   (cb-core)
        // cb-cleaner                 – SDK internal cleaner       (cb-core)
        //
        // These three names are unique to the Couchbase SDK; no other connector produces them.
        // They are owned by SDK-internal static singletons, survive Cluster.disconnect(), and
        // are not connector-specific leak candidates.
        if (threadName.startsWith("SimplePauseDetectorThread")
                || threadName.startsWith("dnsjava NIO selector")
                || threadName.startsWith("cb-cleaner")) {
            return true;
        }
        // parallel-<N> – Reactor parallel scheduler thread.
        //
        // The Couchbase SDK depends on reactor-core but does NOT shade it, so the thread name
        // "parallel-<N>" is identical to the thread name produced by any other connector that
        // also uses reactor-core.  Exempting it by name alone would silently hide leaks from
        // those connectors.
        //
        // Guard: only exempt "parallel-<N>" while the Couchbase E2E test lifecycle is active.
        // CouchbaseIT.startUp() sets the flag via enableCouchbaseParallelThreadExemption() and
        // CouchbaseIT.tearDown() clears it via disableCouchbaseParallelThreadExemption().  Any
        // "parallel-<N>" thread observed outside that window is treated as an unknown thread and
        // reported as a potential leak.
        if (threadName.matches("parallel-\\d+") && couchbaseE2eActive) {
            return true;
        }
        // Azure Queue's shaded Reactor Netty threads are unique to this connector. The
        // boundedElastic evictor name is shared by all Reactor users, so exempt it only while the
        // Azure Queue E2E test is active.
        if (isAzureQueueReactorThreadExempt(threadName)) {
            return true;
        }
        // The shaded GCS client's OpenCensus exporters are JVM-global daemon threads. Their names
        // are shared by other OpenCensus users, so exempt them only for the GCS E2E lifecycle.
        if (isGcsOpenCensusThreadExempt(threadName)) {
            return true;
        }
        // ClickHouse com.clickhouse.client.ClickHouseClientBuilder
        return threadName.startsWith("ClickHouseClientWorker")
                // InfluxDB okio.AsyncTimeout$Watchdog
                || threadName.startsWith("Okio Watchdog")
                // InfluxDB okhttp3.internal.concurrent.TaskRunner.RealBackend
                || threadName.startsWith("OkHttp TaskRunner")
                // IOTDB org.apache.iotdb.session.Session
                || threadName.startsWith("SessionExecutor")
                // Iceberg org.apache.iceberg.util.ThreadPools.WORKER_POOL
                || threadName.startsWith("iceberg-worker-pool")
                // Oracle Driver
                // oracle.jdbc.driver.BlockSource.ThreadedCachingBlockSource.BlockReleaser
                || threadName.contains(
                        "oracle.jdbc.driver.BlockSource.ThreadedCachingBlockSource.BlockReleaser")
                // RocketMQ
                // org.apache.rocketmq.logging.inner.LoggingBuilder$AsyncAppender$Dispatcher
                || threadName.startsWith("AsyncAppender-Dispatcher-Thread")
                // MongoDB
                || threadName.startsWith("BufferPoolPruner")
                || threadName.startsWith("MaintenanceTimer")
                || threadName.startsWith("cluster-")
                // Iceberg
                || threadName.startsWith("iceberg")
                // Iceberg S3 Hadoop catalog
                || threadName.contains("java-sdk-http-connection-reaper")
                || threadName.contains("Timer for 's3a-file-system' metrics system")
                || threadName.startsWith("MutableQuantiles-")
                // JDBC Hana driver
                || threadName.startsWith("Thread-")
                // JNA Cleaner
                || threadName.startsWith("JNA Cleaner")
                // GRPC client
                || threadName.startsWith("grpc")
                // Paimon
                || threadName.startsWith("AsyncOutputStream")
                || threadName.startsWith("MANIFEST-READ-THREAD-POOL")
                // MySQL Connector/J global daemon cleanup thread.
                // Its lifecycle is JVM-level, not tied to any SeaTunnel job.
                // Tracked as a known connector resource leak to be fixed in Phase 3.
                || threadName.contains("abandoned-connection-cleanup");
    }

    static boolean isAzureQueueReactorThreadExempt(String threadName) {
        return threadName.startsWith("org.apache.seatunnel.shade.azure.queue.reactor-http-nio-")
                || (threadName.startsWith("boundedElastic-evictor-") && azureQueueE2eActive);
    }

    static boolean isGcsOpenCensusThreadExempt(String threadName) {
        return gcsE2eActive
                && (threadName.startsWith("ExportComponent.ServiceExporterThread-")
                        || threadName.startsWith("OpenCensus.Disruptor-"));
    }

    @Override
    public Container.ExecResult savepointJob(String jobId)
            throws IOException, InterruptedException {
        return savepointJob(server, jobId);
    }

    @Override
    public Container.ExecResult restoreJob(String confFile, String jobId, String... variables)
            throws IOException, InterruptedException {
        runningCount.incrementAndGet();
        Container.ExecResult result =
                restoreJob(
                        server,
                        confFile,
                        jobId,
                        variables != null ? Arrays.asList(variables) : null);
        runningCount.decrementAndGet();
        return result;
    }

    @Override
    public Container.ExecResult restoreJobWithCheckpoint(
            String confFile, String jobId, String... variables)
            throws IOException, InterruptedException {
        runningCount.incrementAndGet();
        Container.ExecResult result =
                restoreJob(
                        server,
                        confFile,
                        jobId,
                        variables != null ? Arrays.asList(variables) : null,
                        "--restore-with-checkpoint");
        runningCount.decrementAndGet();
        return result;
    }

    @Override
    public Container.ExecResult restoreJobWithCheckpoint(
            String confFile, String sourceJobId, String restoreJobId)
            throws IOException, InterruptedException {
        runningCount.incrementAndGet();
        Container.ExecResult result =
                restoreJob(
                        server,
                        confFile,
                        sourceJobId,
                        restoreJobId,
                        null,
                        "--restore-with-checkpoint");
        runningCount.decrementAndGet();
        return result;
    }

    @Override
    public Container.ExecResult cancelJob(String jobId) throws IOException, InterruptedException {
        return cancelJob(server, jobId);
    }

    @Override
    public void stopJob(String jobId) throws IOException, InterruptedException {
        HttpPost post =
                new HttpPost(
                        String.format(
                                "http://%s:%d%s",
                                server.getHost(), server.getMappedPort(8080), REST_STOP_JOB_PATH));
        ObjectNode requestBody = OBJECT_MAPPER.createObjectNode();
        requestBody.put("jobId", jobId);
        requestBody.put("force", true);
        post.setEntity(new StringEntity(requestBody.toString(), ContentType.APPLICATION_JSON));

        try (CloseableHttpClient client = HttpClients.createDefault()) {
            CloseableHttpResponse response = client.execute(post);
            String responseBody = EntityUtils.toString(response.getEntity());
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != HttpStatus.SC_OK) {
                throw new IOException(
                        String.format("Failed to stop job %s, response: %s", jobId, responseBody));
            }
        }
    }

    @Override
    public String getJobStatus(String jobId) {
        HttpGet get =
                new HttpGet(
                        String.format(
                                "http://%s:%d/job-info/%s",
                                server.getHost(), server.getMappedPort(8080), jobId));
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            CloseableHttpResponse response = client.execute(get);
            if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                String jobStatus = EntityUtils.toString(response.getEntity());
                ObjectNode jsonNodes = JsonUtils.parseObject(jobStatus);
                if (jsonNodes.has("jobStatus")) {
                    return jsonNodes.get("jobStatus").asText();
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    @Override
    public long getCompletedCheckpointCount(String jobId) {
        HttpGet get =
                new HttpGet(
                        String.format(
                                "http://%s:%d%s/%s",
                                server.getHost(),
                                server.getMappedPort(8080),
                                REST_CHECKPOINT_OVERVIEW_PATH,
                                jobId));
        try (CloseableHttpClient client = HttpClients.createDefault()) {
            CloseableHttpResponse response = client.execute(get);
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                return 0L;
            }
            String checkpointOverview = EntityUtils.toString(response.getEntity());
            Map<String, Object> overview =
                    OBJECT_MAPPER.readValue(
                            checkpointOverview, new TypeReference<Map<String, Object>>() {});
            return extractCheckpointCounter(overview, "completed");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SuppressWarnings("unchecked")
    private long extractCheckpointCounter(Map<String, Object> overview, String counterKey) {
        Object pipelinesValue = overview.get("pipelines");
        if (!(pipelinesValue instanceof List) || ((List<?>) pipelinesValue).isEmpty()) {
            return 0L;
        }
        Object pipelineValue = ((List<?>) pipelinesValue).get(0);
        if (!(pipelineValue instanceof Map)) {
            return 0L;
        }
        Object countsValue = ((Map<String, Object>) pipelineValue).get("counts");
        if (!(countsValue instanceof Map)) {
            return 0L;
        }
        Object counter = ((Map<String, Object>) countsValue).get(counterKey);
        return counter instanceof Number ? ((Number) counter).longValue() : 0L;
    }

    @Override
    public String getServerLogs() {
        return server.getLogs();
    }

    @Override
    public void copyFileToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(
                ContainerUtil.getResourcesFile(path).toPath(), targetPath, server);
    }

    @Override
    public void copyAbsolutePathToContainer(String path, String targetPath) {
        ContainerUtil.copyFileIntoContainers(Paths.get(path), targetPath, server);
    }
}
