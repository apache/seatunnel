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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelClientConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointHistoryEntry;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointInfo;
import org.apache.seatunnel.engine.core.checkpoint.CheckpointStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Two-node Zeta master/worker context for repeatedly checkpointing one streaming job. */
@State(Scope.Thread)
public class SeaTunnelCheckpointEnvironmentContext {

    public static final MemorySize DEBLOATING_RECORD_SIZE = MemorySize.parse("1b");
    public static final MemorySize UNALIGNED_RECORD_SIZE = MemorySize.parse("1kb");

    private static final String JOB_TEMPLATE =
            loadTemplate("/benchmark/source-sink-checkpoint.conf.template");
    private static final String ENGINE_CONFIG_TEMPLATE =
            loadTemplate("/benchmark/engine-checkpoint.yaml.template");
    private static final String HAZELCAST_MASTER_CONFIG_TEMPLATE =
            loadTemplate("/benchmark/hazelcast-checkpoint-master.yaml.template");
    private static final String HAZELCAST_WORKER_CONFIG_TEMPLATE =
            loadTemplate("/benchmark/hazelcast-checkpoint-worker.yaml.template");
    private static final Duration START_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration CHECKPOINT_TIMEOUT = Duration.ofMinutes(2);
    private static final Duration CANCEL_TIMEOUT = Duration.ofSeconds(30);
    private static final String DEBLOATING_RECORD_SIZE_VALUE = "1b";
    private static final String UNALIGNED_RECORD_SIZE_VALUE = "1kb";
    private static final long SOURCE_RATE_PER_SECOND = 10_000L;
    private static final int PIPELINE_PARALLELISM = 4;
    private static final int WORKER_SLOT_COUNT = 12;

    @Param({"1b", "1kb"})
    public String recordSize;

    private Path clusterHome;
    private Path checkpointStorageDirectory;
    private Path mapStoreDirectory;
    private SeaTunnelConfig masterConfig;
    private HazelcastInstanceImpl masterInstance;
    private HazelcastInstanceImpl workerInstance;
    private SeaTunnelClient client;
    private ClientJobProxy jobProxy;
    private CheckpointCoordinator checkpointCoordinator;
    private CheckpointMonitorService checkpointMonitorService;
    private int pipelineId;
    private boolean checkpointCompleted;
    private String previousSeaTunnelHome;
    private String previousUppercaseSeaTunnelHome;
    private String previousSeaTunnelConfig;
    private String previousCommonSeaTunnelHome;
    private DeployMode previousDeployMode;

    /** Starts a dedicated master and worker, then submits one long-running benchmark job. */
    @Setup(Level.Trial)
    public void setUp() throws Exception {
        try {
            prepareClusterHome();
            String clusterName = "seatunnel-checkpoint-benchmark-" + UUID.randomUUID();
            writeEngineConfig();

            masterConfig = createMasterConfig(clusterName);
            masterInstance = SeaTunnelServerStarter.createMasterHazelcastInstance(masterConfig);

            int masterPort =
                    masterInstance.getCluster().getLocalMember().getSocketAddress().getPort();
            SeaTunnelConfig workerConfig =
                    createWorkerConfig(clusterName, "127.0.0.1:" + masterPort);
            workerInstance = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerConfig);
            waitUntilClusterFormed();

            ClientConfig clientConfig = new SeaTunnelClientConfig();
            clientConfig.setClusterName(clusterName);
            clientConfig
                    .getNetworkConfig()
                    .setAddresses(Collections.singletonList("127.0.0.1:" + masterPort));
            clientConfig
                    .getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(30_000L);
            client = new SeaTunnelClient(clientConfig);

            Path jobConfigFile = clusterHome.resolve("checkpoint-benchmark.conf");
            Files.write(
                    jobConfigFile, createCheckpointJobConfig().getBytes(StandardCharsets.UTF_8));
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("checkpoint-benchmark-" + UUID.randomUUID());
            ClientJobExecutionEnvironment environment =
                    client.createExecutionContext(
                            jobConfigFile.toString(), jobConfig, masterConfig);
            jobProxy = environment.execute();
            waitUntilRunning();
            resolveCheckpointControl();

            // The MASTER node has no worker slots, so RUNNING proves execution is on WORKER.
            Thread.sleep(2_000L);
        } catch (Exception setupFailure) {
            try {
                tearDown();
            } catch (Exception cleanupFailure) {
                setupFailure.addSuppressed(cleanupFailure);
            }
            throw setupFailure;
        }
    }

    /** Stops the job and both nodes after verifying checkpoint and IMap persistence. */
    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        Exception failure = null;
        try {
            if (checkpointCompleted) {
                verifyCheckpointWasPersisted();
            }
            if (jobProxy != null && !jobProxy.getJobStatus().isEndState()) {
                jobProxy.cancelJob();
                waitUntilCanceled();
            }
        } catch (Exception cleanupFailure) {
            failure = cleanupFailure;
        } finally {
            if (client != null) {
                client.close();
            }
            if (workerInstance != null) {
                workerInstance.shutdown();
            }
            if (masterInstance != null) {
                masterInstance.shutdown();
            }
            restoreEnvironment();
            try {
                deleteRecursively(clusterHome);
            } catch (Exception deletionFailure) {
                if (failure == null) {
                    failure = deletionFailure;
                } else {
                    failure.addSuppressed(deletionFailure);
                }
            }
            clearState();
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Triggers a regular checkpoint and waits until the coordinator records its completion. */
    public void triggerCheckpoint() throws Exception {
        if (checkpointCoordinator == null || checkpointMonitorService == null) {
            throw new IllegalStateException("Checkpoint benchmark job has not been started");
        }
        long previousCheckpointId = latestCompletedCheckpointId();
        PassiveCompletableFuture<CompletedCheckpoint> checkpoint =
                CheckpointBenchmarkTrigger.trigger(checkpointCoordinator);
        checkpoint.get(CHECKPOINT_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        long deadline = System.nanoTime() + CHECKPOINT_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (latestCompletedCheckpointId() > previousCheckpointId
                    && !CheckpointBenchmarkTrigger.hasPendingCheckpoint(checkpointCoordinator)) {
                checkpointCompleted = true;
                return;
            }
            Thread.sleep(1L);
        }
        throw new IllegalStateException("Timed out waiting for benchmark checkpoint to complete");
    }

    String createCheckpointJobConfig() {
        return renderTemplate(
                JOB_TEMPLATE,
                "payload_size",
                selectedRecordSize().getBytes(),
                "source_rate_per_second",
                SOURCE_RATE_PER_SECOND,
                "pipeline_parallelism",
                PIPELINE_PARALLELISM,
                "result_path",
                clusterHome.resolve("checkpoint-results").toAbsolutePath());
    }

    private void prepareClusterHome() throws IOException {
        clusterHome = Files.createTempDirectory("seatunnel-checkpoint-benchmark-");
        Path persistenceDirectory = clusterHome.resolve("checkpoint-persistence");
        checkpointStorageDirectory = persistenceDirectory.resolve("checkpoints");
        mapStoreDirectory = persistenceDirectory.resolve("imap");

        previousSeaTunnelHome = System.getProperty("seatunnel.home");
        previousUppercaseSeaTunnelHome = System.getProperty("SEATUNNEL_HOME");
        previousSeaTunnelConfig = System.getProperty("seatunnel.config");
        previousCommonSeaTunnelHome = Common.getSeaTunnelHome();
        previousDeployMode = Common.getDeployMode();
        System.setProperty("seatunnel.home", clusterHome.toString());
        System.setProperty("SEATUNNEL_HOME", clusterHome.toString());
        Common.setSeaTunnelHome(clusterHome.toString());
        Common.setDeployMode(DeployMode.CLIENT);
    }

    private void writeEngineConfig() throws IOException {
        Path engineConfigFile = clusterHome.resolve("seatunnel.yaml");
        Files.write(
                engineConfigFile,
                renderTemplate(
                                ENGINE_CONFIG_TEMPLATE,
                                "slot_count",
                                WORKER_SLOT_COUNT,
                                "checkpoint_storage_directory",
                                checkpointStorageDirectory.toAbsolutePath())
                        .getBytes(StandardCharsets.UTF_8));
        System.setProperty("seatunnel.config", engineConfigFile.toString());
    }

    private SeaTunnelConfig createMasterConfig(String clusterName) {
        String hazelcastConfig =
                renderTemplate(
                        HAZELCAST_MASTER_CONFIG_TEMPLATE,
                        "cluster_name",
                        clusterName,
                        "imap_directory",
                        mapStoreDirectory.toAbsolutePath());
        return createSeaTunnelConfig(hazelcastConfig);
    }

    private SeaTunnelConfig createWorkerConfig(String clusterName, String masterAddress) {
        String hazelcastConfig =
                renderTemplate(
                        HAZELCAST_WORKER_CONFIG_TEMPLATE,
                        "cluster_name",
                        clusterName,
                        "master_address",
                        masterAddress);
        return createSeaTunnelConfig(hazelcastConfig);
    }

    private SeaTunnelConfig createSeaTunnelConfig(String hazelcastConfig) {
        String engineConfig =
                renderTemplate(
                        ENGINE_CONFIG_TEMPLATE,
                        "slot_count",
                        WORKER_SLOT_COUNT,
                        "checkpoint_storage_directory",
                        checkpointStorageDirectory.toAbsolutePath());
        SeaTunnelConfig config = ConfigProvider.locateAndGetSeaTunnelConfigFromString(engineConfig);
        Config memberConfig =
                new YamlConfigBuilder(
                                new ByteArrayInputStream(
                                        hazelcastConfig.getBytes(StandardCharsets.UTF_8)))
                        .build();
        config.setHazelcastConfig(memberConfig);
        return config;
    }

    private void waitUntilClusterFormed() throws InterruptedException {
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (masterInstance.getCluster().getMembers().size() == 2
                    && workerInstance.getCluster().getMembers().size() == 2) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Timed out forming checkpoint master/worker cluster");
    }

    private void waitUntilRunning() throws InterruptedException {
        long deadline = System.nanoTime() + START_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            JobStatus status = jobProxy.getJobStatus();
            if (status == JobStatus.RUNNING) {
                return;
            }
            if (status.isEndState()) {
                throw new IllegalStateException(
                        "Checkpoint benchmark job ended during setup with " + status);
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("Timed out waiting for checkpoint benchmark job to run");
    }

    private void waitUntilCanceled() throws InterruptedException {
        long deadline = System.nanoTime() + CANCEL_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            if (jobProxy.getJobStatus().isEndState()) {
                return;
            }
            Thread.sleep(10L);
        }
        throw new IllegalStateException("Timed out canceling checkpoint benchmark job");
    }

    private void resolveCheckpointControl() {
        SeaTunnelServer server =
                masterInstance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
        if (!server.isMasterNode()) {
            throw new IllegalStateException("Checkpoint benchmark master node is not active");
        }
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobProxy.getJobId());
        if (jobMaster == null) {
            throw new IllegalStateException("Checkpoint benchmark JobMaster is unavailable");
        }
        List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
        if (pipelines.size() != 1) {
            throw new IllegalStateException(
                    "Checkpoint benchmark requires one pipeline but found " + pipelines.size());
        }
        pipelineId = pipelines.get(0).getPipelineId();
        checkpointCoordinator =
                jobMaster.getCheckpointManager().getCheckpointCoordinator(pipelineId);
        checkpointMonitorService = server.getCheckpointMonitorService();
    }

    private long latestCompletedCheckpointId() {
        return checkpointMonitorService
                .getHistory(jobProxy.getJobId(), pipelineId, 1, CheckpointStatus.COMPLETED).stream()
                .map(CheckpointHistoryEntry::getCheckpointInfo)
                .mapToLong(CheckpointInfo::getCheckpointId)
                .findFirst()
                .orElse(-1L);
    }

    private void verifyCheckpointWasPersisted() throws Exception {
        verifyPersistenceDirectory(checkpointStorageDirectory, ".ser", "checkpoint");
        verifyPersistenceDirectory(mapStoreDirectory, null, "MapStore");
    }

    private static void verifyPersistenceDirectory(
            Path directory, String fileSuffix, String storageName) throws Exception {
        if (directory == null || !Files.isDirectory(directory)) {
            throw new IllegalStateException(
                    storageName + " storage directory was not created: " + directory);
        }
        try (Stream<Path> files = Files.walk(directory)) {
            if (files.noneMatch(
                    path ->
                            Files.isRegularFile(path)
                                    && (fileSuffix == null
                                            || path.getFileName()
                                                    .toString()
                                                    .endsWith(fileSuffix)))) {
                throw new IllegalStateException(
                        "No persisted " + storageName + " state was found under " + directory);
            }
        }
    }

    private MemorySize selectedRecordSize() {
        if (DEBLOATING_RECORD_SIZE_VALUE.equals(recordSize)) {
            return DEBLOATING_RECORD_SIZE;
        }
        if (UNALIGNED_RECORD_SIZE_VALUE.equals(recordSize)) {
            return UNALIGNED_RECORD_SIZE;
        }
        throw new IllegalArgumentException("Unsupported checkpoint record size: " + recordSize);
    }

    /** Immutable binary memory size used only by this checkpoint environment. */
    public static final class MemorySize {

        private static final Pattern SIZE_PATTERN = Pattern.compile("([0-9]+)\\s*([a-zA-Z]+)");

        private final long bytes;

        private MemorySize(long bytes) {
            this.bytes = bytes;
        }

        public static MemorySize parse(String value) {
            Matcher matcher = SIZE_PATTERN.matcher(value.trim());
            if (!matcher.matches()) {
                throw new IllegalArgumentException("Invalid memory size: " + value);
            }

            long amount = Long.parseLong(matcher.group(1));
            String unit = matcher.group(2).toLowerCase(Locale.ROOT);
            long multiplier;
            switch (unit) {
                case "b":
                    multiplier = 1L;
                    break;
                case "kb":
                case "kib":
                    multiplier = 1L << 10;
                    break;
                case "mb":
                case "mib":
                    multiplier = 1L << 20;
                    break;
                case "gb":
                case "gib":
                    multiplier = 1L << 30;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported memory size unit: " + unit);
            }
            return new MemorySize(Math.multiplyExact(amount, multiplier));
        }

        public long getBytes() {
            return bytes;
        }
    }

    private void restoreEnvironment() {
        restoreSystemProperty("seatunnel.home", previousSeaTunnelHome);
        restoreSystemProperty("SEATUNNEL_HOME", previousUppercaseSeaTunnelHome);
        restoreSystemProperty("seatunnel.config", previousSeaTunnelConfig);
        Common.setSeaTunnelHome(previousCommonSeaTunnelHome);
        Common.setDeployMode(previousDeployMode);
    }

    private void clearState() {
        clusterHome = null;
        checkpointStorageDirectory = null;
        mapStoreDirectory = null;
        masterConfig = null;
        masterInstance = null;
        workerInstance = null;
        client = null;
        jobProxy = null;
        checkpointCoordinator = null;
        checkpointMonitorService = null;
        checkpointCompleted = false;
    }

    private static String loadTemplate(String resourceName) {
        InputStream input =
                SeaTunnelCheckpointEnvironmentContext.class.getResourceAsStream(resourceName);
        if (input == null) {
            throw new IllegalStateException("Benchmark template was not found: " + resourceName);
        }
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            return reader.lines().collect(Collectors.joining("\n", "", "\n"));
        } catch (IOException e) {
            throw new IllegalStateException("Could not read benchmark template " + resourceName, e);
        }
    }

    private static String renderTemplate(String template, Object... replacements) {
        String rendered = template;
        for (int index = 0; index < replacements.length; index += 2) {
            rendered =
                    rendered.replace(
                            "{{" + replacements[index] + "}}",
                            String.valueOf(replacements[index + 1]));
        }
        if (rendered.contains("{{")) {
            throw new IllegalStateException(
                    "Checkpoint benchmark template contains an unresolved placeholder");
        }
        return rendered;
    }

    private static void restoreSystemProperty(String key, String value) {
        if (value == null) {
            System.clearProperty(key);
        } else {
            System.setProperty(key, value);
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (root == null || !Files.exists(root)) {
            return;
        }
        List<Path> paths;
        try (Stream<Path> stream = Files.walk(root)) {
            paths = stream.sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        }
        for (Path path : paths) {
            Files.deleteIfExists(path);
        }
    }
}
