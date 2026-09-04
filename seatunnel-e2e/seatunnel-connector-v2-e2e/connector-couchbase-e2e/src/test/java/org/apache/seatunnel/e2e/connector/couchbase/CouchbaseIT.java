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

package org.apache.seatunnel.e2e.connector.couchbase;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * End-to-end integration test for the Couchbase sink connector.
 *
 * <p>Starts a real Couchbase community server via {@link CouchbaseContainer}, which handles full
 * cluster bootstrap automatically (node init, services, memory quotas, credentials, bucket). The
 * test then creates one scoped collection, writes 100 rows through SeaTunnel using the FakeSource
 * connector, and asserts both the exact row count and a content-level check on one document.
 */
@Slf4j
public class CouchbaseIT extends TestSuiteBase implements TestResource {

    private static final String COUCHBASE_IMAGE = "couchbase/server:community-7.1.1";
    private static final String COUCHBASE_CONTAINER_HOST = "e2e_couchbase";
    private static final String COUCHBASE_USERNAME = "Administrator";
    private static final String COUCHBASE_PASSWORD = "password";
    private static final String COUCHBASE_BUCKET = "test_bucket";
    private static final String COUCHBASE_SCOPE = "_default";
    private static final String COUCHBASE_COLLECTION = "test_collection";
    private static final String COUCHBASE_COLLECTION_TIMER_FLUSH = "test_collection_timer_flush";

    /** Matches row.num in fake_source_to_couchbase.conf. */
    private static final int EXPECTED_ROW_COUNT = 100;

    private CouchbaseContainer couchbaseContainer;
    private Cluster cluster;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        // Scope the parallel-N thread exemption to the Couchbase E2E lifecycle. The Couchbase SDK
        // uses unshaded reactor-core, so its Reactor scheduler threads are named "parallel-<N>" —
        // the same name as threads from any other Reactor-based connector. The exemption must only
        // fire while this test is running to avoid masking leaks in unrelated connectors.
        SeaTunnelContainer.enableCouchbaseParallelThreadExemption();
        couchbaseContainer =
                new CouchbaseContainer(DockerImageName.parse(COUCHBASE_IMAGE))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(COUCHBASE_CONTAINER_HOST)
                        .withCredentials(COUCHBASE_USERNAME, COUCHBASE_PASSWORD)
                        .withBucket(
                                new BucketDefinition(COUCHBASE_BUCKET)
                                        .withReplicas(0)
                                        .withQuota(256)
                                        .withPrimaryIndex(false))
                        .withEnabledServices(
                                CouchbaseService.KV, CouchbaseService.QUERY, CouchbaseService.INDEX)
                        // Testcontainers 1.17.6 calls the Couchbase REST API (renameNode) during
                        // containerIsStarting, immediately after the HTTP port responds. On loaded
                        // CI runners with newer Docker the REST daemon is not yet fully ready at
                        // that point, causing a Connection reset / unexpected end of stream error.
                        // A 3-minute startup timeout gives the HttpWaitStrategy enough retry
                        // budget to ride out the transient unavailability window.
                        .withStartupTimeout(Duration.ofMinutes(3))
                        // The startup timeout above only governs the wait strategy; the
                        // containerIsStarting REST bootstrap has no retry of its own, and its
                        // Connection reset failures kept killing the single default start attempt
                        // on CI (seen repeatedly on ubuntu-latest since 2026-08-15, including on
                        // apache/dev push builds). Whole-container retries cover that window with
                        // a fresh server each attempt.
                        .withStartupAttempts(3)
                        // Surface the server's own stdout/stderr in the CI job log; without it a
                        // bootstrap failure only shows the client-side socket error and gives no
                        // way to see why the couchbase daemon dropped the connection.
                        .withLogConsumer(new Slf4jLogConsumer(log).withPrefix("couchbase-server"));
        couchbaseContainer.start();

        cluster =
                Cluster.connect(
                        couchbaseContainer.getConnectionString(),
                        couchbaseContainer.getUsername(),
                        couchbaseContainer.getPassword());

        // Wait for the query/management service to be ready before issuing DDL. The container
        // signals readiness at the KV/bucket level but the query and index services can still
        // reject requests for a short window after Cluster.connect() returns. Wrapping the DDL
        // itself in a retry loop is the safest approach — it eliminates the startup timing race
        // without relying on a fixed sleep.

        String createCollectionDdl =
                "CREATE COLLECTION `"
                        + COUCHBASE_BUCKET
                        + "`.`"
                        + COUCHBASE_SCOPE
                        + "`.`"
                        + COUCHBASE_COLLECTION
                        + "`";
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query(createCollectionDdl));

        String createIndexDdl =
                "CREATE PRIMARY INDEX ON `"
                        + COUCHBASE_BUCKET
                        + "`.`"
                        + COUCHBASE_SCOPE
                        + "`.`"
                        + COUCHBASE_COLLECTION
                        + "`";
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query(createIndexDdl));

        String indexStatusQuery =
                String.format(
                        "SELECT state FROM system:indexes WHERE keyspace_id = '%s'"
                                + " AND `using` = 'gsi' AND is_primary = true"
                                + " AND `bucket_id` = '%s'",
                        COUCHBASE_COLLECTION, COUCHBASE_BUCKET);
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            QueryResult r = cluster.query(indexStatusQuery);
                            List<JsonObject> rows = r.rowsAs(JsonObject.class);
                            Assertions.assertFalse(rows.isEmpty(), "Primary index not created yet");
                            Assertions.assertEquals(
                                    "online",
                                    rows.get(0).getString("state"),
                                    "Primary index not yet online");
                        });

        // Similarly retry CREATE PRIMARY INDEX until the collection is visible to the query path.
        String createCollectionDdlTimerFlush =
                "CREATE COLLECTION `"
                        + COUCHBASE_BUCKET
                        + "`.`"
                        + COUCHBASE_SCOPE
                        + "`.`"
                        + COUCHBASE_COLLECTION_TIMER_FLUSH
                        + "`";
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query(createCollectionDdlTimerFlush));

        String createIndexDdlTimerFlush =
                "CREATE PRIMARY INDEX ON `"
                        + COUCHBASE_BUCKET
                        + "`.`"
                        + COUCHBASE_SCOPE
                        + "`.`"
                        + COUCHBASE_COLLECTION_TIMER_FLUSH
                        + "`";
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query(createIndexDdlTimerFlush));

        String indexStatusQueryTimerFlush =
                String.format(
                        "SELECT state FROM system:indexes WHERE keyspace_id = '%s'"
                                + " AND `using` = 'gsi' AND is_primary = true"
                                + " AND `bucket_id` = '%s'",
                        COUCHBASE_COLLECTION_TIMER_FLUSH, COUCHBASE_BUCKET);
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            QueryResult r = cluster.query(indexStatusQueryTimerFlush);
                            List<JsonObject> rows = r.rowsAs(JsonObject.class);
                            Assertions.assertFalse(rows.isEmpty(), "Primary index not created yet");
                            Assertions.assertEquals(
                                    "online",
                                    rows.get(0).getString("state"),
                                    "Primary index not yet online");
                        });
        log.info("Couchbase cluster ready at {}", couchbaseContainer.getConnectionString());
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            if (cluster != null) {
                cluster.disconnect();
            }
            if (couchbaseContainer != null) {
                couchbaseContainer.stop();
            }
        } finally {
            // Always clear the flag so that parallel-N threads are no longer exempt after
            // this test class finishes, regardless of whether teardown succeeded.
            SeaTunnelContainer.disableCouchbaseParallelThreadExemption();
        }
    }

    /**
     * Verifies that FakeSource rows are correctly written to the Couchbase collection.
     *
     * <p>Assertions:
     *
     * <ol>
     *   <li>Exact row count matches {@code row.num} from the job config (100).
     *   <li>Content check: one written document contains all expected fields.
     * </ol>
     */
    @TestTemplate
    public void testFakeSourceToCouchbaseSink(TestContainer container)
            throws IOException, InterruptedException {
        // Purge any documents left by a previous container iteration so the COUNT assertion
        // always starts from zero regardless of how many Flink/Spark versions are under test.
        cluster.query(
                String.format(
                        "DELETE FROM `%s`.`%s`.`%s`",
                        COUCHBASE_BUCKET, COUCHBASE_SCOPE, COUCHBASE_COLLECTION));

        Container.ExecResult execResult = container.executeJob("/fake_source_to_couchbase.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        // The Flink/Spark container writes via the Docker-internal hostname (e2e_couchbase) while
        // the test JVM connects via the mapped port. Because REQUEST_PLUS only waits for mutations
        // seen by *this* SDK connection, it cannot observe the Flink/Spark writes. We instead poll
        // with Awaitility until the index catches up — the same pattern used across this codebase
        // for cross-process writes.
        String countQuery =
                String.format(
                        "SELECT COUNT(*) AS cnt FROM `%s`.`%s`.`%s`",
                        COUCHBASE_BUCKET, COUCHBASE_SCOPE, COUCHBASE_COLLECTION);
        // FakeSource now uses auto.increment.enabled=true starting at 1, so ids are 1..100
        // and there are no collisions.  Pick id=1 for the content-level check.
        // Note: the document key for id=1 is "1:1" (length-prefixed encoding: "<len>:<value>"),
        // not "1". The content check queries by field value (WHERE id = 1) via N1QL rather than
        // by document key, so the encoding does not affect correctness here.
        String contentQuery =
                String.format(
                        "SELECT id, name, score, `active` FROM `%s`.`%s`.`%s` WHERE id = 1",
                        COUCHBASE_BUCKET, COUCHBASE_SCOPE, COUCHBASE_COLLECTION);

        // --- Assertion 1: exact row count ---
        AtomicInteger count = new AtomicInteger();
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            QueryResult result = cluster.query(countQuery);
                            List<JsonObject> rows = result.rowsAs(JsonObject.class);
                            Assertions.assertFalse(rows.isEmpty(), "COUNT query returned no rows");
                            count.set(rows.get(0).getInt("cnt"));
                            Assertions.assertEquals(
                                    EXPECTED_ROW_COUNT,
                                    count.get(),
                                    "Document count mismatch: expected="
                                            + EXPECTED_ROW_COUNT
                                            + " actual="
                                            + count.get());
                        });

        // --- Assertion 2: content check on the document keyed by id=1 ---
        // primary-key=["id"] in conf and auto.increment.start=1, so doc key "1" is guaranteed.
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            QueryResult result = cluster.query(contentQuery);
                            List<JsonObject> rows = result.rowsAs(JsonObject.class);
                            Assertions.assertFalse(
                                    rows.isEmpty(), "Content query returned no document");
                            JsonObject doc = rows.get(0);
                            Assertions.assertTrue(
                                    doc.containsKey("id"),
                                    "Field 'id' missing in written document");
                            Assertions.assertNotNull(
                                    doc.getString("name"),
                                    "Field 'name' missing in written document");
                            Assertions.assertNotNull(
                                    doc.getDouble("score"),
                                    "Field 'score' missing in written document");
                            log.info(
                                    "E2E passed: count={}, sample doc={}",
                                    count.get(),
                                    doc.toMap());
                        });
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason =
                    "engine-level timer flush (sink.flush.interval) is only supported on Zeta engine")
    public void testCouchbaseSinkTimerFlush(TestContainer container) throws Exception {
        cluster.query(
                String.format(
                        "DELETE FROM `%s`.`%s`.`%s`",
                        COUCHBASE_BUCKET, COUCHBASE_SCOPE, COUCHBASE_COLLECTION_TIMER_FLUSH));

        String jobId = String.valueOf(System.currentTimeMillis());
        java.util.concurrent.CompletableFuture<Container.ExecResult> jobFuture =
                java.util.concurrent.CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/fake_source_to_couchbase_timer_flush.conf", jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        String countQuery =
                String.format(
                        "SELECT COUNT(*) AS cnt FROM `%s`.`%s`.`%s`",
                        COUCHBASE_BUCKET, COUCHBASE_SCOPE, COUCHBASE_COLLECTION_TIMER_FLUSH);

        try {
            Awaitility.given()
                    .ignoreExceptions()
                    .pollInterval(2, TimeUnit.SECONDS)
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertFalse(
                                        jobFuture.isDone(),
                                        "The streaming job must still be running when timer flush publishes the buffered rows");
                                QueryResult result = cluster.query(countQuery);
                                List<JsonObject> rows = result.rowsAs(JsonObject.class);
                                Assertions.assertFalse(
                                        rows.isEmpty(), "COUNT query returned no rows");
                                Assertions.assertEquals(10, rows.get(0).getInt("cnt"));
                            });
        } finally {
            if (!jobFuture.isDone()) {
                Container.ExecResult cancelResult = container.cancelJob(jobId);
                Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());
            }
        }
    }
}
