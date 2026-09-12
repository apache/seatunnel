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
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelClientConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * JMH environment that owns an embedded, single-node Zeta cluster and its client.
 *
 * <p>The cluster is started once per JMH trial, while every benchmark invocation submits and waits
 * for one complete bounded job. Cluster startup and shutdown are therefore outside the measured
 * section. Engine-level benchmark capabilities such as checkpoint control, fault injection, and
 * metrics collection should be integrated here instead of in individual benchmark classes.
 */
@State(Scope.Thread)
public class SeaTunnelEnvironmentContext {

    public static final String RESULT_DIRECTORY_PROPERTY = "seatunnel.benchmark.result.dir";

    protected static final int SLOT_COUNT = 12;
    private static final long SOURCE_START_DELAY_MILLIS = 250L;
    private static final int SOURCE_EMIT_BATCH_SIZE = 1_024;
    private static final String SOURCE_SINK_JOB_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/source-sink.conf.template");
    private static final String SOURCE_TRANSFORM_SINK_JOB_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/source-transform-sink.conf.template");
    private static final String ENGINE_CONFIG_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/engine.yaml.template");
    protected static final String BENCHMARK_TRANSFORM_NAME = "benchmark_transform";

    private final AtomicLong invocationSequence = new AtomicLong();

    private Path miniClusterHome;
    private Path resultDirectory;
    private SeaTunnelConfig seaTunnelConfig;
    private HazelcastInstance miniCluster;
    private SeaTunnelClient client;
    private String clusterName;
    private String previousSeaTunnelHome;
    private String previousUppercaseSeaTunnelHome;
    private String previousSeaTunnelConfig;
    private String previousCommonSeaTunnelHome;
    private DeployMode previousDeployMode;
    private boolean measurementIteration = true;

    @Setup(Level.Trial)
    public void setUp() throws Exception {
        if (miniCluster != null) {
            throw new IllegalStateException("SeaTunnel mini cluster was already started");
        }

        miniClusterHome = Files.createTempDirectory("seatunnel-benchmark-minicluster-");
        resultDirectory = resolveResultDirectory();
        Files.createDirectories(resultDirectory);

        previousSeaTunnelHome = System.getProperty("seatunnel.home");
        previousUppercaseSeaTunnelHome = System.getProperty("SEATUNNEL_HOME");
        previousSeaTunnelConfig = System.getProperty("seatunnel.config");
        previousCommonSeaTunnelHome = Common.getSeaTunnelHome();
        previousDeployMode = Common.getDeployMode();
        System.setProperty("seatunnel.home", miniClusterHome.toString());
        System.setProperty("SEATUNNEL_HOME", miniClusterHome.toString());
        Path engineConfigFile = miniClusterHome.resolve("seatunnel.yaml");
        Files.write(
                engineConfigFile, embeddedEngineConfiguration().getBytes(StandardCharsets.UTF_8));
        System.setProperty("seatunnel.config", engineConfigFile.toString());
        Common.setSeaTunnelHome(miniClusterHome.toString());
        Common.setDeployMode(DeployMode.CLIENT);

        try {
            clusterName = "seatunnel-benchmark-" + UUID.randomUUID();
            seaTunnelConfig = createSeaTunnelConfig(clusterName);
            miniCluster = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            int memberPort = miniCluster.getCluster().getLocalMember().getSocketAddress().getPort();
            ClientConfig clientConfig = new SeaTunnelClientConfig();
            clientConfig.setClusterName(clusterName);
            clientConfig
                    .getNetworkConfig()
                    .setAddresses(Collections.singletonList("127.0.0.1:" + memberPort));
            clientConfig
                    .getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(30_000L);
            client = new SeaTunnelClient(clientConfig);
        } catch (Exception startupFailure) {
            try {
                tearDown();
            } catch (Exception cleanupFailure) {
                startupFailure.addSuppressed(cleanupFailure);
            }
            throw startupFailure;
        }
    }

    /** Records whether the current JMH iteration contributes to the published benchmark report. */
    @Setup(Level.Iteration)
    public void setUpIteration(IterationParams iterationParams) {
        measurementIteration = iterationParams.getType() == IterationType.MEASUREMENT;
    }

    @TearDown(Level.Trial)
    public void tearDown() throws Exception {
        try {
            if (client != null) {
                client.close();
            }
        } finally {
            client = null;
            try {
                if (miniCluster != null) {
                    miniCluster.shutdown();
                }
            } finally {
                miniCluster = null;
                restoreSystemProperty("seatunnel.home", previousSeaTunnelHome);
                restoreSystemProperty("SEATUNNEL_HOME", previousUppercaseSeaTunnelHome);
                restoreSystemProperty("seatunnel.config", previousSeaTunnelConfig);
                Common.setSeaTunnelHome(previousCommonSeaTunnelHome);
                Common.setDeployMode(previousDeployMode);
                deleteRecursively(miniClusterHome);
                miniClusterHome = null;
            }
        }
    }

    /** Submits one bounded pipeline and blocks until Zeta reaches a terminal state. */
    public BenchmarkRunResult execute(BenchmarkPipeline pipeline, PipelineBenchmarkOptions options)
            throws Exception {
        try {
            return executeInternal(pipeline, options);
        } catch (Exception failure) {
            try {
                tearDown();
            } catch (Exception cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private BenchmarkRunResult executeInternal(
            BenchmarkPipeline pipeline, PipelineBenchmarkOptions options) throws Exception {
        ensureStarted();
        if (options.getParallelism() > SLOT_COUNT) {
            throw new IllegalArgumentException(
                    "parallelism "
                            + options.getParallelism()
                            + " exceeds mini cluster slot count "
                            + SLOT_COUNT);
        }

        String runId =
                pipeline.getId()
                        + '-'
                        + invocationSequence.incrementAndGet()
                        + '-'
                        + Long.toUnsignedString(System.nanoTime());
        Path jobConfigFile = miniClusterHome.resolve("jobs").resolve(runId + ".conf");
        Files.createDirectories(jobConfigFile.getParent());
        Files.write(
                jobConfigFile,
                createJobConfig(pipeline, options, runId).getBytes(StandardCharsets.UTF_8));

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(runId);
        try {
            ClientJobExecutionEnvironment environment =
                    client.createExecutionContext(
                            jobConfigFile.toString(), jobConfig, seaTunnelConfig);
            JobResult jobResult = environment.execute().waitForJobCompleteV2();
            if (jobResult.getStatus() != JobStatus.FINISHED) {
                throw new IllegalStateException(
                        "Benchmark job "
                                + runId
                                + " ended with "
                                + jobResult.getStatus()
                                + ": "
                                + jobResult.getError());
            }
        } finally {
            Files.deleteIfExists(jobConfigFile);
        }

        Path resultFile = resultDirectory.resolve(runId + ".json");
        if (!Files.isRegularFile(resultFile)) {
            throw new IllegalStateException(
                    "Benchmark sink did not produce result file " + resultFile);
        }
        BenchmarkRunResult result = BenchmarkRunResult.read(resultFile);
        validateResult(pipeline, options, result);
        if (!measurementIteration) {
            Files.delete(resultFile);
        }
        return result;
    }

    protected SeaTunnelConfig createSeaTunnelConfig(String name) {
        SeaTunnelConfig config = new SeaTunnelConfig();
        config.getHazelcastConfig().setClusterName(name);
        config.getHazelcastConfig().setProperty("hazelcast.phone.home.enabled", "false");
        config.getHazelcastConfig().getNetworkConfig().setPortAutoIncrement(true);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getInterfaces()
                .setEnabled(true)
                .addInterface("127.0.0.1");
        JoinConfig join = config.getHazelcastConfig().getNetworkConfig().getJoin();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(false);
        EngineConfig engineConfig = config.getEngineConfig();
        engineConfig.setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        engineConfig.setMode(ExecutionMode.LOCAL);
        engineConfig.setBackupCount(0);
        engineConfig.setClassloaderCacheMode(true);
        engineConfig.getSlotServiceConfig().setDynamicSlot(false);
        engineConfig.getSlotServiceConfig().setSlotNum(SLOT_COUNT);
        engineConfig.getHttpConfig().setEnabled(false);
        engineConfig.getHttpConfig().setEnableHttps(false);
        return config;
    }

    String createJobConfig(
            BenchmarkPipeline pipeline, PipelineBenchmarkOptions options, String runId) {
        return BenchmarkTemplates.render(
                jobConfigTemplate(pipeline),
                "job_name",
                escapeConfigString(runId),
                "parallelism",
                options.getParallelism(),
                "total_rows",
                options.getTotalRows(),
                "rate_per_second",
                options.getOfferedRatePerSecond(),
                "payload_size",
                options.getPayloadSize(),
                "start_delay_millis",
                SOURCE_START_DELAY_MILLIS,
                "emit_batch_size",
                SOURCE_EMIT_BATCH_SIZE,
                "result_path",
                escapeConfigString(resultDirectory.toString()),
                "run_id",
                escapeConfigString(runId),
                "transform_name",
                BENCHMARK_TRANSFORM_NAME,
                "transform_operations",
                options.getTransformOperations());
    }

    /** Selects the complete job configuration template for this benchmark environment. */
    protected String jobConfigTemplate(BenchmarkPipeline pipeline) {
        return pipeline.isTransformEnabled()
                ? SOURCE_TRANSFORM_SINK_JOB_TEMPLATE
                : SOURCE_SINK_JOB_TEMPLATE;
    }

    /** Renders the complete engine configuration for this benchmark environment. */
    protected String embeddedEngineConfiguration() {
        return BenchmarkTemplates.render(ENGINE_CONFIG_TEMPLATE, "slot_count", SLOT_COUNT);
    }

    private static void validateResult(
            BenchmarkPipeline pipeline,
            PipelineBenchmarkOptions options,
            BenchmarkRunResult result) {
        if (result.getExpectedRows() != options.getTotalRows()
                || result.getProcessedRows() != options.getTotalRows()) {
            throw new IllegalStateException(
                    "Incomplete benchmark output: expected="
                            + options.getTotalRows()
                            + ", processed="
                            + result.getProcessedRows());
        }
        if (pipeline.isTransformEnabled() && result.getChecksum() == 0L) {
            throw new IllegalStateException("Transform pipeline produced an empty checksum");
        }
        if (!pipeline.isTransformEnabled() && result.getChecksum() != 0L) {
            throw new IllegalStateException("Direct pipeline unexpectedly changed the checksum");
        }
    }

    private void ensureStarted() {
        if (miniCluster == null || client == null) {
            throw new IllegalStateException("SeaTunnel mini cluster has not been started");
        }
    }

    private static Path resolveResultDirectory() {
        String configured = System.getProperty(RESULT_DIRECTORY_PROPERTY);
        if (configured != null && !configured.trim().isEmpty()) {
            return Paths.get(configured).toAbsolutePath();
        }
        return Paths.get("seatunnel-benchmarks", "target", "pipeline-results").toAbsolutePath();
    }

    private static String escapeConfigString(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    protected final Path getMiniClusterHome() {
        if (miniClusterHome == null) {
            throw new IllegalStateException("SeaTunnel mini cluster home has not been created");
        }
        return miniClusterHome;
    }

    /** Returns the embedded member to benchmark environments that exercise engine internals. */
    protected final HazelcastInstance getMiniCluster() {
        return miniCluster;
    }

    /** Returns the embedded client to benchmark environments that prepare real engine fixtures. */
    protected final SeaTunnelClient getClient() {
        return client;
    }

    /** Returns the configuration used by the embedded benchmark member and client. */
    protected final SeaTunnelConfig getSeaTunnelConfig() {
        return seaTunnelConfig;
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
