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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.rest.RestConstant;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.layout.PatternLayout;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngine;
import io.restassured.common.mapper.TypeRef;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;

/**
 * Regression test for the Zeta engine master-node cold-start {@code NullPointerException} described
 * in <a href="https://github.com/apache/seatunnel/issues/10570">issue #10570</a> and fixed by <a
 * href="https://github.com/apache/seatunnel/pull/10610">PR #10610</a> ("Prevent NPE by lazy
 * initializing overviewMap").
 *
 * <p><b>The regression.</b> Before the fix, {@code CheckpointMonitorService}'s constructor eagerly
 * resolved its backing Hazelcast {@code IMap}:
 *
 * <pre>{@code
 * private final IMap<Long, CheckpointOverview> overviewMap;
 *
 * public CheckpointMonitorService(NodeEngine nodeEngine, int maxHistorySize) {
 *     this.overviewMap = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_CHECKPOINT_MONITOR);
 *     this.maxHistorySize = maxHistorySize;
 * }
 * }</pre>
 *
 * That constructor runs on every master-role node's cold start, from {@code
 * SeaTunnelServer#startMaster()} (called by {@code SeaTunnelServer#init(NodeEngine, Properties)},
 * today at seatunnel-engine-server's {@code SeaTunnelServer.java:166/172} and {@code
 * SeaTunnelServer.java:202}). {@code init(NodeEngine, Properties)} is a Hazelcast {@code
 * ManagedService} callback that Hazelcast itself invokes from {@code
 * ServiceManagerImpl#initServices()}, which runs inside {@code NodeEngineImpl#start()}, which runs
 * inside {@code Node#start()}, which runs <em>inside {@code HazelcastInstanceImpl}'s own
 * constructor</em> - i.e. before the Hazelcast instance has finished constructing itself and before
 * its partition/operation infrastructure is ready. The reporter's config additionally set a
 * Hazelcast map-store with {@code initial-mode: EAGER} on the {@code engine*} map family (covering
 * {@code engine_checkpoint_monitor}), which makes {@code IMap} proxy creation synchronously call
 * {@code waitUntilLoaded()} -&gt; {@code invokeOnPartition()} -&gt; {@code new
 * PartitionInvocation()} -&gt; {@code new Invocation()}; that constructor NPEs because it depends
 * on partition/operation state this early bootstrap window has not published yet. Because {@code
 * init()} is one sequential method and SeaTunnel's own Jetty REST server is started strictly
 * <em>after</em> {@code startMaster()} inside that same method (today {@code
 * SeaTunnelServer.java:187-192}), the uncaught NPE aborted {@code init()} before the Jetty-start
 * lines ever ran. That is the precise mechanism behind the bug title "blocking Jetty and other
 * services initialization": the NPE never touches Jetty's own code, it simply prevents every line
 * of {@code init()} written after {@code startMaster()} - Jetty included - from executing at all.
 *
 * <p><b>The fix and its current shape.</b> PR #10610 moved the {@code getMap()} call out of the
 * constructor into a lazily-invoked, double-checked-locked accessor, so the constructor - which
 * still runs inside the risky {@code Node#start()} window - only stores references, and the actual
 * Hazelcast map access happens on first genuine use, by which time the node has long finished
 * starting and joined the cluster. That exact idiom is still present, verbatim in shape, in the
 * current {@code CheckpointMonitorService} (seatunnel-engine-server's {@code
 * checkpoint/monitor/CheckpointMonitorService.java:54-72}), though the codebase has since grown a
 * state-store abstraction layer: the field's static type changed from a raw {@code IMap<Long,
 * CheckpointOverview>} to a {@code CheckpointOverviewStateStore}, obtained through {@code
 * engineContext.getStateStores().auxiliary().checkpointOverviewStateStore()}. That indirection
 * (implemented by {@code HazelcastEngineStateStores}) independently defers its own four {@code
 * getMap()} calls behind an analogous {@code ensureInitialized()} double-checked lock, so the
 * lazy-initialization discipline this fix introduced has since been generalized rather than
 * abandoned. There is no separate feature flag gating any of this: {@code CheckpointMonitorService}
 * is constructed unconditionally by {@code startMaster()} for every node whose {@code cluster-role}
 * includes {@code MASTER} - which is the engine's default {@code MASTER_AND_WORKER} role - so a
 * plain multi-node cluster start already exercises the exact call chain the bug lived in.
 *
 * <p><b>What this test proves, and how.</b> Reproducing the reporter's literal trigger (a Hazelcast
 * map-store configured with {@code initial-mode: EAGER} against a real backing store) is not
 * practical from this test tree: no fixture here wires up a working {@code MapStoreFactory}, and
 * inventing one purely for this test would be a materially new harness rather than a reuse of this
 * initiative's established in-process cluster conventions. Instead this test combines two
 * complementary, source-grounded checks:
 *
 * <ol>
 *   <li>A precise structural check, via reflection, on the exact field PR #10610 introduced ({@code
 *       CheckpointMonitorService#overviewMap}): immediately after {@code
 *       SeaTunnelServerStarter#createHazelcastInstance(SeaTunnelConfig)} returns for every
 *       concurrently-started node - i.e. the instant {@code Node#start()} and therefore {@code
 *       init()}/{@code startMaster()} have fully completed - the field must still be {@code null}.
 *       A regression that reintroduced eager initialization at the constructor call site would make
 *       this assertion fail deterministically, independent of whether the specific NPE reproduces
 *       in this environment.
 *   <li>An end-to-end functional proof, mirroring {@link
 *       org.apache.seatunnel.engine.e2e.telemetry.TelemetryStartupDeadlockIT} (issue #10840 / PR
 *       #10841), this initiative's closest prior example of a startup-race between an ancillary
 *       service and the rest of cluster bring-up: {@value #NODE_COUNT} nodes are started genuinely
 *       concurrently (real contention on the exact ordering this bug depended on), every node's
 *       Jetty/REST endpoint must answer within a bounded timeout immediately afterwards (the
 *       literal "Jetty ... initialization" symptom from the issue), a real streaming job must reach
 *       {@code RUNNING}, a real checkpoint must complete (exercising {@code
 *       CheckpointMonitorService#onCheckpointTriggered}/{@code #onCheckpointCompleted} -&gt; {@code
 *       #getOverviewMap()} for real, well after startup), and the checkpoint-overview REST endpoint
 *       must answer correctly on every node afterwards - matching the cross-node convention already
 *       established by {@link RestApiIT#testCheckpointOverviewAndHistoryApi()}. A log4j2 appender
 *       attached to the root logger (the engine routes Hazelcast's {@code ILogger} through log4j2,
 *       and this appender technique otherwise mirrors {@code
 *       TaskDeploymentPrePublicationClassLoaderLeakIT}'s {@code EngineLogCapture}) independently
 *       asserts no {@code NullPointerException} was logged anywhere during the whole sequence.
 * </ol>
 *
 * <p><b>Verification methodology and its limits.</b> Per this session's constraints, no local Maven
 * build or test execution was available to empirically revert the fix and confirm this test fails
 * without it, the way {@code TelemetryStartupDeadlockIT} documents doing for its own regression;
 * this test's correctness instead rests on tracing the exact constructor/{@code init()}/Jetty
 * ordering directly in the current source (file:line references above), so GitHub CI on the pull
 * request built from this commit is this code's first execution.
 */
@Slf4j
public class CheckpointMonitorStartupRaceIT {

    /**
     * Number of nodes started concurrently. More than the bare minimum of 2 so the concurrent
     * {@code Node#start()} calls create genuine multi-node contention around cluster join and
     * master-role activation, closer to a real "cluster cold start" than a single lone node would
     * be, matching {@code TelemetryStartupDeadlockIT}'s {@code NODE_COUNT}.
     */
    private static final int NODE_COUNT = 3;

    /**
     * Bound for each individual node's {@code createHazelcastInstance()} call. Generous relative to
     * a normal single-node startup so this shared machine's contention does not fail the test, but
     * still finite so a genuine startup hang (or a regression that turns the NPE into a permanently
     * stuck bootstrap instead of a thrown exception) fails the test instead of hanging CI. Matches
     * {@code TelemetryStartupDeadlockIT}'s equivalent budget.
     */
    private static final long NODE_START_TIMEOUT_SECONDS = 60L;

    /**
     * Bound for all {@value #NODE_COUNT} nodes to observe each other as cluster members. Matches
     * {@code TelemetryStartupDeadlockIT}'s equivalent budget for the same kind of check.
     */
    private static final long CLUSTER_READY_TIMEOUT_SECONDS = 60L;

    /**
     * Bound for each node's Jetty/REST endpoint to answer once {@code createHazelcastInstance()}
     * has already returned for that node. {@code JettyService#createJettyServer()} calls {@code
     * Server#start()} synchronously inside {@code SeaTunnelServer#init()}, so the endpoint should
     * already be serving by the time this wait begins; the bound exists only to absorb local
     * socket-readiness jitter on a loaded CI machine, not because any real delay is expected.
     */
    private static final long JETTY_READY_TIMEOUT_SECONDS = 30L;

    /**
     * Bound for the submitted job to reach {@code RUNNING}. Matches the 2-minute budget already
     * used by {@code TelemetryStartupDeadlockIT} and {@code JobLogUrlPortIT} for the same kind of
     * check.
     */
    private static final long JOB_RUNNING_TIMEOUT_MINUTES = 2L;

    /**
     * Bound for the job's first checkpoint to complete. {@code stream_fakesource_to_console.conf}
     * sets {@code checkpoint.interval = 5000}, so a completed checkpoint should appear well inside
     * this window; matches the 2-minute budget {@link
     * RestApiIT#testCheckpointOverviewAndHistoryApi()} already uses while polling the identical
     * "completed count" condition through the same REST endpoint.
     */
    private static final long CHECKPOINT_COMPLETED_TIMEOUT_MINUTES = 2L;

    /**
     * Starts {@value #NODE_COUNT} Hazelcast nodes genuinely concurrently - exercising the exact
     * {@code init()}/{@code startMaster()}/{@code CheckpointMonitorService} constructor ordering
     * issue #10570 lived in - then proves the lazy-initialization contract PR #10610 introduced
     * holds both structurally (the backing field stays unset through the risky startup window) and
     * functionally (Jetty comes up on every node, a real job runs, a real checkpoint completes, and
     * every node's checkpoint REST endpoint answers correctly afterwards).
     *
     * @throws Exception if node startup, job submission, or the reflective field checks fail
     *     unexpectedly
     */
    @Test
    public void testConcurrentClusterStartupAvoidsCheckpointMonitorNpeAndStaysFunctional()
            throws Exception {
        String testClusterName = TestUtils.getClusterName("CheckpointMonitorStartupRaceIT");

        List<HazelcastInstanceImpl> nodes = new CopyOnWriteArrayList<>();
        List<Integer> httpPorts = new ArrayList<>();
        ExecutorService nodeStartExecutor = Executors.newFixedThreadPool(NODE_COUNT);
        SeaTunnelClient engineClient = null;

        try (NpeLogCapture logCapture = NpeLogCapture.install()) {
            // Build every node's config, including its own pre-allocated free HTTP port, up
            // front and sequentially - before any concurrency starts - so NODE_COUNT nodes
            // starting Jetty at the same instant never race over the same port, and so
            // createHazelcastInstance() calls below race against each other on cluster join and
            // master-role activation exactly like a real concurrent cold start, not against this
            // setup step.
            List<SeaTunnelConfig> nodeConfigs = new ArrayList<>();
            for (int i = 0; i < NODE_COUNT; i++) {
                int httpPort = getAvailablePort();
                httpPorts.add(httpPort);
                nodeConfigs.add(buildNodeConfig(testClusterName, httpPort));
            }

            // Genuinely concurrent startup: submit every createHazelcastInstance() call at once
            // so each node's Node#start() - and therefore its SeaTunnelServer#init()/
            // #startMaster()/CheckpointMonitorService construction - races the others exactly
            // like the issue's "cluster cold start" scenario. If a regression reintroduced eager
            // state-store access in the constructor, this call throws (propagating out of
            // future.get() below) instead of returning a healthy instance.
            List<CompletableFuture<HazelcastInstanceImpl>> nodeFutures = new ArrayList<>();
            for (SeaTunnelConfig nodeConfig : nodeConfigs) {
                nodeFutures.add(
                        CompletableFuture.supplyAsync(
                                () -> SeaTunnelServerStarter.createHazelcastInstance(nodeConfig),
                                nodeStartExecutor));
            }
            for (CompletableFuture<HazelcastInstanceImpl> future : nodeFutures) {
                nodes.add(future.get(NODE_START_TIMEOUT_SECONDS, TimeUnit.SECONDS));
            }

            // Precise structural check on the exact field PR #10610 made lazy. By the time
            // createHazelcastInstance() has returned for a node, that node's Node#start() -
            // and therefore the CheckpointMonitorService constructor called a moment ago from
            // startMaster() - has already fully completed. A non-null field here means the
            // constructor eagerly touched the checkpoint state store again, reintroducing the
            // exact ordering hazard the original fix removed, regardless of whether it happens
            // to NPE in this particular environment.
            for (HazelcastInstanceImpl node : nodes) {
                CheckpointMonitorService monitorService = getCheckpointMonitorService(node);
                Assertions.assertNotNull(
                        monitorService,
                        "a MASTER_AND_WORKER node must construct a CheckpointMonitorService in"
                                + " startMaster()");
                Assertions.assertNull(
                        readOverviewMapField(monitorService),
                        "overviewMap must stay unset until first genuine use; a non-null value"
                                + " immediately after startup means the constructor eagerly"
                                + " touched the checkpoint state store again, reintroducing the"
                                + " startup-ordering hazard PR #10610 removed");
            }

            Awaitility.await()
                    .atMost(CLUSTER_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            NODE_COUNT,
                                            nodes.get(0).getCluster().getMembers().size(),
                                            "all nodes must converge into a single cluster"));

            // Jetty must already be serving on every node. JettyService#createJettyServer()
            // calls Server#start() synchronously inside SeaTunnelServer#init(), strictly after
            // startMaster() returns. Before the fix, an NPE thrown out of startMaster() aborted
            // init() before this line ever ran - the literal "blocking Jetty ... initialization"
            // symptom from issue #10570. A regression here would make every one of these checks
            // fail, not merely run slowly.
            for (int httpPort : httpPorts) {
                String baseUrl = buildHttpBaseUrl(httpPort);
                Awaitility.await()
                        .atMost(JETTY_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        given().get(baseUrl + RestConstant.REST_URL_OVERVIEW)
                                                .then()
                                                .statusCode(200));
            }

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(testClusterName);
            engineClient = new SeaTunnelClient(clientConfig);

            String confPath = TestUtils.getResource("stream_fakesource_to_console.conf");
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("CheckpointMonitorStartupRaceIT_job");
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(confPath, jobConfig, nodeConfigs.get(0));
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            long jobId = clientJobProxy.getJobId();

            Awaitility.await()
                    .atMost(JOB_RUNNING_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            JobStatus.RUNNING, clientJobProxy.getJobStatus()));

            // Prove the lazily-obtained state store is genuinely functional, not just "never
            // crashed": stream_fakesource_to_console.conf sets checkpoint.interval = 5000, so
            // wait for a real checkpoint to complete. This exercises CheckpointCoordinator ->
            // CheckpointMonitorService#onCheckpointTriggered/#onCheckpointCompleted ->
            // #getOverviewMap()'s double-checked-locked accessor for real, well after startup -
            // the exact deferred access path the fix relies on.
            String primaryBaseUrl = buildHttpBaseUrl(httpPorts.get(0));
            Awaitility.await()
                    .atMost(CHECKPOINT_COMPLETED_TIMEOUT_MINUTES, TimeUnit.MINUTES)
                    .until(() -> getCompletedCheckpointCount(jobId, primaryBaseUrl) > 0L);

            // Query every node's own REST endpoint, matching
            // RestApiIT#testCheckpointOverviewAndHistoryApi's cross-node convention: the
            // checkpoint overview state store is a distributed Hazelcast-backed map, so every
            // node must answer identically and without error regardless of which one actually
            // hosts this job's CheckpointCoordinator.
            for (int httpPort : httpPorts) {
                String baseUrl = buildHttpBaseUrl(httpPort);
                Awaitility.await()
                        .atMost(JETTY_READY_TIMEOUT_SECONDS, TimeUnit.SECONDS)
                        .untilAsserted(
                                () ->
                                        Assertions.assertTrue(
                                                getCompletedCheckpointCount(jobId, baseUrl) > 0L,
                                                "node at "
                                                        + baseUrl
                                                        + " must report the completed checkpoint"
                                                        + " through its own REST endpoint"));
            }

            // The REST queries above forced a first genuine getOverviewMap() call on every node
            // (not only the one running this job's CheckpointCoordinator), so the field must now
            // be populated everywhere - completing the before/after proof that the fix defers
            // initialization without ever skipping it.
            for (HazelcastInstanceImpl node : nodes) {
                CheckpointMonitorService monitorService = getCheckpointMonitorService(node);
                Assertions.assertNotNull(
                        readOverviewMapField(monitorService),
                        "overviewMap must be populated once genuinely used");
            }

            Assertions.assertFalse(
                    logCapture.anyLineContains("NullPointerException"),
                    "no NullPointerException may be logged anywhere during cluster startup, job"
                            + " execution, or checkpointing; captured warnings/errors: "
                            + logCapture.snapshot());
        } finally {
            nodeStartExecutor.shutdownNow();
            if (engineClient != null) {
                engineClient.close();
            }
            for (HazelcastInstanceImpl node : nodes) {
                if (node != null) {
                    node.shutdown();
                }
            }
        }
    }

    /**
     * Builds an independent {@link SeaTunnelConfig} for one node: a unique cluster name so parallel
     * test runs do not collide, and SeaTunnel's own Jetty REST server enabled on a pre-allocated
     * free port - the opposite of {@code TelemetryStartupDeadlockIT}'s config, since this test's
     * whole point is proving Jetty is reachable, not avoiding a port clash by disabling it.
     *
     * @param clusterName the shared Hazelcast cluster name every node must join
     * @param httpPort the free port this node's embedded Jetty server should bind
     * @return a freshly-loaded config, independent from any other node's
     */
    private SeaTunnelConfig buildNodeConfig(String clusterName, int httpPort) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(true);
        seaTunnelConfig.getEngineConfig().getHttpConfig().setPort(httpPort);
        return seaTunnelConfig;
    }

    /**
     * Finds a currently-free local TCP port by briefly binding an ephemeral one and releasing it,
     * matching the convention already established by {@code RealtimeMetricsRestIT}. Each of this
     * test's {@value #NODE_COUNT} nodes needs its own port since all nodes run in this one JVM.
     *
     * @return a port that was free at the moment of the check
     */
    private static int getAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            throw new IllegalStateException(
                    "No available port for CheckpointMonitorStartupRaceIT", e);
        }
    }

    /**
     * Builds the base REST URL for a node's embedded Jetty server, matching {@code
     * RestApiIT#buildHttpBaseUrl(int)}'s convention of appending the (here, default-empty) engine
     * {@code context-path} after the host and port.
     *
     * @param httpPort the node's Jetty port
     * @return a base URL such as {@code http://localhost:18080}
     */
    private String buildHttpBaseUrl(int httpPort) {
        return "http://localhost:" + httpPort;
    }

    /**
     * Resolves the given node's {@link SeaTunnelServer} instance and returns its {@link
     * CheckpointMonitorService}, following the same {@code
     * NodeEngine#getService(SeaTunnelServer.SERVICE_NAME)} idiom already used by {@code
     * TaskDeploymentPrePublicationClassLoaderLeakIT}.
     *
     * @param node the in-process Hazelcast node to inspect
     * @return that node's {@code CheckpointMonitorService}
     */
    private static CheckpointMonitorService getCheckpointMonitorService(
            HazelcastInstanceImpl node) {
        NodeEngine nodeEngine = node.node.getNodeEngine();
        SeaTunnelServer seaTunnelServer = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        return seaTunnelServer.getCheckpointMonitorService();
    }

    /**
     * Reads the private {@code overviewMap} field PR #10610 made {@code volatile} and
     * lazily-initialized, without invoking its accessor (which would populate it as a side effect).
     * This is the one field the fix targeted, so its nullness is the most direct possible signal of
     * whether the constructor still defers initialization the way the fix requires.
     *
     * @param monitorService the service instance to inspect
     * @return the field's current value: {@code null} until first genuine use, non-null afterwards
     * @throws ReflectiveOperationException if the field cannot be found or read; would indicate an
     *     unrelated structural change to {@code CheckpointMonitorService}, not a false positive on
     *     the field's value
     */
    private static Object readOverviewMapField(CheckpointMonitorService monitorService)
            throws ReflectiveOperationException {
        Field overviewMapField = CheckpointMonitorService.class.getDeclaredField("overviewMap");
        overviewMapField.setAccessible(true);
        return overviewMapField.get(monitorService);
    }

    /**
     * Fetches the checkpoint overview for the given job from the given node's REST endpoint and
     * returns the first pipeline's completed-checkpoint count, following the same {@code
     * REST_URL_CHECKPOINT_OVERVIEW} response shape {@code
     * RestApiIT#testCheckpointOverviewAndHistoryApi()} already parses.
     *
     * @param jobId the running job's id
     * @param baseUrl the querying node's REST base URL
     * @return the {@code completed} count for the job's first pipeline, or {@code 0} if the
     *     overview has no pipelines yet
     */
    @SuppressWarnings("unchecked")
    private static long getCompletedCheckpointCount(long jobId, String baseUrl) {
        Map<String, Object> overview =
                given().get(baseUrl + RestConstant.REST_URL_CHECKPOINT_OVERVIEW + "/" + jobId)
                        .then()
                        .statusCode(200)
                        .extract()
                        .as(new TypeRef<Map<String, Object>>() {});
        List<Map<String, Object>> pipelines = (List<Map<String, Object>>) overview.get("pipelines");
        if (pipelines == null || pipelines.isEmpty()) {
            return 0L;
        }
        Map<String, Object> counts = (Map<String, Object>) pipelines.get(0).get("counts");
        if (counts == null) {
            return 0L;
        }
        Object completedValue = counts.get("completed");
        return completedValue instanceof Number ? ((Number) completedValue).longValue() : 0L;
    }

    /**
     * Captures every {@code WARN}-and-above log event across the whole JVM for the duration of this
     * test, so a {@code NullPointerException} logged by Hazelcast's own internals (as the original
     * bug report shows, through Hazelcast's {@code ProxyService} and {@code ServiceManagerImpl},
     * not through any SeaTunnel-owned logger name) is caught alongside anything a future regression
     * might log through {@code CheckpointMonitorService} itself. Unlike {@code
     * TaskDeploymentPrePublicationClassLoaderLeakIT}'s {@code EngineLogCapture} - which registers
     * dedicated {@code INFO}-level configs for two already-known logger names - this appender
     * attaches directly to the root {@link LoggerConfig} at the {@code WARN} threshold the active
     * test log4j2 configuration already applies to every logger, so it needs no new logger
     * configuration and cannot change what is already printed to the console.
     */
    private static final class NpeLogCapture extends AbstractAppender implements AutoCloseable {

        private final LoggerContext loggerContext;
        private final LoggerConfig rootLoggerConfig;

        /** One entry per captured event: logger name, message, and full stack trace, if any. */
        private final List<String> lines = new CopyOnWriteArrayList<>();

        private NpeLogCapture(LoggerContext loggerContext, LoggerConfig rootLoggerConfig) {
            super(
                    "CheckpointMonitorStartupRaceIT-capture",
                    null,
                    PatternLayout.createDefaultLayout(),
                    false,
                    Property.EMPTY_ARRAY);
            this.loggerContext = loggerContext;
            this.rootLoggerConfig = rootLoggerConfig;
        }

        static NpeLogCapture install() {
            LoggerContext loggerContext = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = loggerContext.getConfiguration();
            LoggerConfig rootLoggerConfig = configuration.getRootLogger();
            NpeLogCapture capture = new NpeLogCapture(loggerContext, rootLoggerConfig);
            capture.start();
            rootLoggerConfig.addAppender(capture, Level.WARN, null);
            loggerContext.updateLoggers();
            return capture;
        }

        @Override
        public void append(LogEvent event) {
            StringBuilder line =
                    new StringBuilder(event.getLoggerName())
                            .append(' ')
                            .append(event.getMessage().getFormattedMessage());
            Throwable thrown = event.getThrown();
            if (thrown != null) {
                // A full printStackTrace() dump (not just Throwable#toString()) is required here:
                // the original bug's NPE carries no message, and the class name that matters
                // (CheckpointMonitorService) only appears in a stack frame, never in the
                // exception's own toString() or in ServiceManagerImpl's log message text.
                StringWriter stackTrace = new StringWriter();
                thrown.printStackTrace(new PrintWriter(stackTrace));
                line.append(" | ").append(stackTrace);
            }
            lines.add(line.toString());
        }

        /**
         * Whether any captured line contains all of the given fragments.
         *
         * @param fragments substrings that must all be present in the same line
         * @return {@code true} if at least one captured line contains every fragment
         */
        boolean anyLineContains(String... fragments) {
            return lines.stream()
                    .anyMatch(line -> Arrays.stream(fragments).allMatch(line::contains));
        }

        /**
         * Returns an immutable snapshot of every captured line, for inclusion in assertion failure
         * messages.
         *
         * @return the captured lines, in capture order
         */
        List<String> snapshot() {
            return Collections.unmodifiableList(new ArrayList<>(lines));
        }

        @Override
        public void close() {
            rootLoggerConfig.removeAppender(getName());
            loggerContext.updateLoggers();
            stop();
        }
    }
}
