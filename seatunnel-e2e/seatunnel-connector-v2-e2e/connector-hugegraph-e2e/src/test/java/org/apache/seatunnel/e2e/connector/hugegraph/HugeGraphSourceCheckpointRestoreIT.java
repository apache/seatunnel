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

package org.apache.seatunnel.e2e.connector.hugegraph;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.hugegraph.driver.HugeClient;
import org.apache.hugegraph.structure.constant.IdStrategy;
import org.apache.hugegraph.structure.graph.Vertex;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Engine-level checkpoint/restore verification for the HugeGraph bounded source.
 *
 * <p>Unlike the plain source E2Es (bounded round-trip) and the unit-level state round-trip, this
 * exercises the real Zeta runtime: a small {@code page_size} forces multiple pages (so several
 * checkpoints persist the opaque page token), the Console sink is throttled so the bounded scan
 * stays RUNNING long enough to take a savepoint mid-scan, and the job is then restored from that
 * savepoint. Every emitted {@code ~id} across both runs is collected from the cluster log and the
 * union is asserted to contain each vertex exactly once — proving the restored reader neither
 * re-reads (no duplicate) nor skips (no loss) across the page boundary.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Savepoint/restore is driven through the SeaTunnel Zeta engine CLI only.")
public class HugeGraphSourceCheckpointRestoreIT extends TestSuiteBase implements TestResource {

    private static final String HUGE_GRAPH_IMAGE = "hugegraph/hugegraph:1.5.0";
    private static final String HUGE_GRAPH_HOST = "hugegraph-host";
    private static final int HUGE_GRAPH_PORT = 8080;
    private static final String GRAPH_NAME = "hugegraph";
    private static final String VERTEX_LABEL = "person";
    private static final String CONF_FILE = "/hugegraph/hugegraph_source_checkpoint_restore.conf";

    // Distinctive name prefix so emitted rows can be isolated in the shared cluster log.
    private static final String NAME_PREFIX = "cp-person-";
    // Must exceed one page (page_size=5 in the conf) so the scan spans several pages/checkpoints.
    private static final int VERTEX_COUNT = 40;

    private GenericContainer<?> hugeGraphContainer;
    private HugeClient hugeClient;

    @BeforeAll
    @Override
    public void startUp() {
        hugeGraphContainer =
                new GenericContainer<>(DockerImageName.parse(HUGE_GRAPH_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HUGE_GRAPH_HOST)
                        .withExposedPorts(HUGE_GRAPH_PORT)
                        .waitingFor(
                                Wait.forHttp("/graphs").forPort(HUGE_GRAPH_PORT).forStatusCode(200))
                        .withStartupTimeout(Duration.ofMinutes(3));
        Startables.deepStart(Stream.of(hugeGraphContainer)).join();

        String url =
                String.format(
                        "http://%s:%d",
                        hugeGraphContainer.getHost(),
                        hugeGraphContainer.getMappedPort(HUGE_GRAPH_PORT));
        hugeClient = HugeClient.builder(url, GRAPH_NAME).build();
    }

    @TestTemplate
    public void testSourceCheckpointRestoreNoDuplicateNoLoss(TestContainer container)
            throws Exception {
        clearGraph();
        setupSchema();
        for (int i = 0; i < VERTEX_COUNT; i++) {
            hugeClient
                    .graph()
                    .addVertex(
                            new Vertex(VERTEX_LABEL)
                                    .property("name", NAME_PREFIX + i)
                                    .property("age", 29));
        }
        awaitTotalVertexCount(VERTEX_COUNT);

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        boolean restoreCompleted = false;
        CompletableFuture<Container.ExecResult> restoreFuture = null;

        CompletableFuture<Container.ExecResult> firstRun =
                CompletableFuture.supplyAsync(() -> executeJob(container, CONF_FILE, jobId));

        try {
            Awaitility.await()
                    .atMost(90, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "RUNNING",
                                            container.getJobStatus(jobId),
                                            "Source job must reach RUNNING before savepoint"));

            // Wait until at least a full first page has been emitted, so the savepoint is
            // guaranteed to land after at least one page-boundary checkpoint (i.e. mid-scan).
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .until(() -> countEmittedIds(container).total >= 5);

            Container.ExecResult savepoint = container.savepointJob(jobId);
            Assertions.assertEquals(0, savepoint.getExitCode(), savepoint.getStderr());

            Container.ExecResult firstResult = waitForJobResult(firstRun);
            Assertions.assertEquals(0, firstResult.getExitCode(), firstResult.getStderr());

            restoreFuture =
                    CompletableFuture.supplyAsync(() -> restoreJob(container, CONF_FILE, jobId));

            Container.ExecResult restoreResult = waitForJobResult(restoreFuture);
            Assertions.assertEquals(0, restoreResult.getExitCode(), restoreResult.getStderr());
            restoreCompleted = true;

            EmittedIds emitted = countEmittedIds(container);
            Assertions.assertEquals(
                    VERTEX_COUNT,
                    emitted.distinct,
                    "Restored scan lost rows: expected every vertex id exactly once across the two"
                            + " runs, distinct ids seen were fewer than the loaded count");
            Assertions.assertEquals(
                    VERTEX_COUNT,
                    emitted.total,
                    "Restored scan produced duplicates: total emitted rows across the two runs must"
                            + " equal the loaded count (no page was re-read across the savepoint)");
        } finally {
            if (!restoreCompleted) {
                try {
                    container.cancelJob(jobId);
                } catch (Exception ignored) {
                    // best effort: the job may already have terminated
                }
                if (!firstRun.isDone()) {
                    waitForJobResult(firstRun);
                }
                if (restoreFuture != null && !restoreFuture.isDone()) {
                    waitForJobResult(restoreFuture);
                }
            }
        }
    }

    /** Collected view of the {@code ~id} column values printed by the Console sink. */
    private static final class EmittedIds {
        final int total;
        final int distinct;

        EmittedIds(int total, int distinct) {
            this.total = total;
            this.distinct = distinct;
        }
    }

    /**
     * Parses the Console sink output from the cluster log. Each printed row looks like {@code ...
     * SeaTunnelRow#kind=INSERT : <~id>, <~label>, <name>, <age>}; the first field is the vertex id.
     * Both runs write to the same cluster log, so this naturally spans savepoint and restore.
     */
    private EmittedIds countEmittedIds(TestContainer container) {
        List<String> ids = new ArrayList<>();
        for (String line : container.getServerLogs().split("\\R")) {
            if (!line.contains("SeaTunnelRow#kind=") || !line.contains(NAME_PREFIX)) {
                continue;
            }
            int marker = line.lastIndexOf(" : ");
            if (marker < 0) {
                continue;
            }
            String fields = line.substring(marker + 3);
            String id = fields.split(", ")[0].trim();
            if (!id.isEmpty()) {
                ids.add(id);
            }
        }
        Set<String> distinct = new HashSet<>(ids);
        return new EmittedIds(ids.size(), distinct.size());
    }

    private Container.ExecResult executeJob(TestContainer container, String conf, String jobId) {
        try {
            return container.executeJob(conf, jobId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Container.ExecResult restoreJob(TestContainer container, String conf, String jobId) {
        try {
            return container.restoreJob(conf, jobId);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Container.ExecResult waitForJobResult(CompletableFuture<Container.ExecResult> future) {
        try {
            return future.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void clearGraph() {
        hugeClient.graphs().clearGraph(GRAPH_NAME, "I'm sure to delete all data");
    }

    private void setupSchema() {
        hugeClient.schema().propertyKey("name").asText().ifNotExist().create();
        hugeClient.schema().propertyKey("age").asInt().ifNotExist().create();
        hugeClient
                .schema()
                .vertexLabel(VERTEX_LABEL)
                .idStrategy(IdStrategy.PRIMARY_KEY)
                .primaryKeys("name")
                .properties("name", "age")
                .nullableKeys("age")
                .ifNotExist()
                .create();
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> hugeClient.schema().getVertexLabel(VERTEX_LABEL) != null);
    }

    private void awaitTotalVertexCount(int expectedCount) {
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .until(
                        () ->
                                hugeClient
                                                .graph()
                                                .listVertices(
                                                        VERTEX_LABEL,
                                                        java.util.Collections.emptyMap(),
                                                        expectedCount + 1)
                                                .size()
                                        == expectedCount);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (hugeClient != null) {
            hugeClient.close();
        }
        if (hugeGraphContainer != null) {
            hugeGraphContainer.close();
        }
    }
}
