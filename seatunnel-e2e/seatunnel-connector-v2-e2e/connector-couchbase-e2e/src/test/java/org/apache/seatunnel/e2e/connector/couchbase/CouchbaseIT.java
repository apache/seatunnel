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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.couchbase.source.CouchbaseSourceFactory;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

import com.couchbase.client.core.error.UnambiguousTimeoutException;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryScanConsistency;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * End-to-end integration test for the Couchbase source and sink connector.
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
    private static final String COUCHBASE_COLLECTION_SOURCE_RESULT = "test_source_result";
    private static final String COUCHBASE_SOURCE_SCOPE = "source_scope";
    private static final String COUCHBASE_SOURCE_COLLECTION = "source_collection";
    private static final String COUCHBASE_TIMEOUT_COLLECTION = "source_timeout";
    private static final int TIMEOUT_ROW_COUNT = 512;
    private static final int SMALL_QUERY_TIMEOUT_SECONDS = 5;
    private static final int SLOW_COLLECTOR_DELAY_SECONDS = 10;

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

        // The container's HTTP bootstrap may finish before the KV service accepts authenticated
        // client connections. Confirm KV readiness before a SeaTunnel job uses the Docker alias.
        cluster.bucket(COUCHBASE_BUCKET).waitUntilReady(Duration.ofMinutes(2));

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
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                cluster.query(
                                        "CREATE COLLECTION `test_bucket`.`_default`.`test_source_result`"));
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                cluster.query(
                                        "CREATE PRIMARY INDEX ON `test_bucket`.`_default`.`test_source_result`"));
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query("CREATE SCOPE `test_bucket`.`source_scope`"));
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                cluster.query(
                                        "CREATE COLLECTION `test_bucket`.`source_scope`.`source_collection`"));
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(2, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                cluster.query(
                                        "CREATE PRIMARY INDEX ON `test_bucket`.`source_scope`.`source_collection`"));
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            QueryResult result =
                                    cluster.query(
                                            "SELECT state FROM system:indexes"
                                                    + " WHERE keyspace_id = 'source_collection'"
                                                    + " AND scope_id = 'source_scope'"
                                                    + " AND bucket_id = 'test_bucket'"
                                                    + " AND `using` = 'gsi' AND is_primary = true");
                            List<JsonObject> indexes = result.rowsAs(JsonObject.class);
                            Assertions.assertFalse(
                                    indexes.isEmpty(), "Source index not created yet");
                            Assertions.assertEquals(
                                    "online",
                                    indexes.get(0).getString("state"),
                                    "Source index not yet online");
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
    public void testCouchbaseSourceToAssert(TestContainer container)
            throws IOException, InterruptedException {
        cluster.query(
                "DELETE FROM `test_bucket`.`_default`.`test_source_result`",
                QueryOptions.queryOptions().scanConsistency(QueryScanConsistency.REQUEST_PLUS));
        Collection sourceCollection =
                cluster.bucket(COUCHBASE_BUCKET)
                        .scope(COUCHBASE_SOURCE_SCOPE)
                        .collection(COUCHBASE_SOURCE_COLLECTION);
        sourceCollection.upsert(
                "source-one",
                JsonObject.create()
                        .put("id", 1)
                        .put("name", "first")
                        .put("enabled", true)
                        .put("amount", 12.5)
                        .putNull("nullable"));
        sourceCollection.upsert(
                "source-two",
                JsonObject.create()
                        .put("id", 2)
                        .put("name", "second")
                        .put("enabled", false)
                        .put("amount", -3.25));

        Container.ExecResult result = container.executeJob("/couchbase_source_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(1, TimeUnit.SECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<JsonObject> rows =
                                    cluster.query(
                                                    "SELECT RAW d FROM `test_bucket`.`_default`.`"
                                                            + COUCHBASE_COLLECTION_SOURCE_RESULT
                                                            + "` AS d ORDER BY d.id",
                                                    QueryOptions.queryOptions()
                                                            .scanConsistency(
                                                                    QueryScanConsistency
                                                                            .REQUEST_PLUS))
                                            .rowsAsObject();
                            Assertions.assertEquals(2, rows.size());
                            Assertions.assertEquals(1, rows.get(0).getInt("id"));
                            Assertions.assertEquals("first", rows.get(0).getString("name"));
                            Assertions.assertEquals(true, rows.get(0).getBoolean("enabled"));
                            Assertions.assertEquals(12.5, rows.get(0).getDouble("amount"));
                            Assertions.assertNull(rows.get(0).get("nullable"));
                            Assertions.assertEquals(2, rows.get(1).getInt("id"));
                            Assertions.assertEquals("second", rows.get(1).getString("name"));
                            Assertions.assertEquals(false, rows.get(1).getBoolean("enabled"));
                            Assertions.assertEquals(-3.25, rows.get(1).getDouble("amount"));
                            Assertions.assertNull(rows.get(1).get("nullable"));
                        });
    }

    /** Verifies the server's streaming timeout through the production reader, without an engine. */
    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    public void testCouchbaseSourceStreamingTimeoutAndReplay() throws Exception {
        String path = "`test_bucket`.`source_scope`.`source_timeout`";
        Awaitility.given()
                .ignoreExceptions()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query("CREATE COLLECTION " + path));
        Awaitility.given()
                .ignoreExceptions()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> cluster.query("CREATE PRIMARY INDEX ON " + path));
        Collection collection =
                cluster.bucket(COUCHBASE_BUCKET)
                        .scope(COUCHBASE_SOURCE_SCOPE)
                        .collection(COUCHBASE_TIMEOUT_COLLECTION);
        // 64 MiB of results prevents SDK prefetch and socket buffers from hiding backpressure.
        // Sleeping on a tiny, already-buffered response would not exercise a server timeout.
        String payload = "x".repeat(128 * 1024);
        for (int id = 0; id < TIMEOUT_ROW_COUNT; id++) {
            collection.upsert(
                    "timeout-" + id, JsonObject.create().put("id", id).put("payload", payload));
        }
        Awaitility.given()
                .ignoreExceptions()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        TIMEOUT_ROW_COUNT,
                                        cluster.query(
                                                        "SELECT RAW COUNT(*) FROM " + path,
                                                        QueryOptions.queryOptions()
                                                                .scanConsistency(
                                                                        QueryScanConsistency
                                                                                .REQUEST_PLUS))
                                                .rowsAs(Integer.class)
                                                .get(0)));

        Set<Integer> expectedIds =
                IntStream.range(0, TIMEOUT_ROW_COUNT).boxed().collect(Collectors.toSet());
        SourceReader.Context fastContext = mock(SourceReader.Context.class);
        List<Integer> fast = new ArrayList<>();
        try (SourceReader<SeaTunnelRow, SingleSplit> reader = timeoutReader(60, fastContext)) {
            reader.addSplits(Collections.singletonList(new SingleSplit(null)));
            reader.open();
            // Allow CI headroom: this control checks complete results, not throughput.
            reader.pollNext(collectorWithInitialDelay(fast, 0));
            Assertions.assertEquals(TIMEOUT_ROW_COUNT, fast.size());
            Assertions.assertEquals(expectedIds, new HashSet<>(fast));
            verify(fastContext).signalNoMoreElement();
        }

        SourceReader.Context failedContext = mock(SourceReader.Context.class);
        List<Integer> partial = new ArrayList<>();
        List<SingleSplit> state;
        try (SourceReader<SeaTunnelRow, SingleSplit> reader =
                timeoutReader(SMALL_QUERY_TIMEOUT_SECONDS, failedContext)) {
            reader.addSplits(Collections.singletonList(new SingleSplit(null)));
            reader.open();
            UnambiguousTimeoutException failure =
                    Assertions.assertThrows(
                            UnambiguousTimeoutException.class,
                            () ->
                                    reader.pollNext(
                                            collectorWithInitialDelay(
                                                    partial, SLOW_COLLECTOR_DELAY_SECONDS)));
            Assertions.assertTrue(
                    failure.getMessage().contains("Query timed out while streaming/receiving rows"),
                    "Must observe server error 1080, not a timeout before streaming starts");
            Assertions.assertFalse(partial.isEmpty(), "Must emit rows before the timeout");
            Assertions.assertTrue(partial.size() < TIMEOUT_ROW_COUNT);
            verify(failedContext, never()).signalNoMoreElement();
            state = reader.snapshotState(1);
            Assertions.assertNull(state.get(0).getState(), "No offset is checkpointed");
        }

        SourceReader.Context restoredContext = mock(SourceReader.Context.class);
        List<Integer> completed = new ArrayList<>();
        try (SourceReader<SeaTunnelRow, SingleSplit> reader = timeoutReader(60, restoredContext)) {
            reader.addSplits(state);
            reader.open();
            // Same fixture and slow collector; only the query budget changes.
            reader.pollNext(collectorWithInitialDelay(completed, SLOW_COLLECTOR_DELAY_SECONDS));
            Assertions.assertEquals(TIMEOUT_ROW_COUNT, completed.size());
            Assertions.assertEquals(expectedIds, new HashSet<>(completed));
            Assertions.assertTrue(completed.containsAll(partial), "Recovery replays emitted rows");
            verify(restoredContext).signalNoMoreElement();
        }
    }

    private SourceReader<SeaTunnelRow, SingleSplit> timeoutReader(
            int timeoutSeconds, SourceReader.Context context) throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put("connection.string", couchbaseContainer.getConnectionString());
        options.put("username", COUCHBASE_USERNAME);
        options.put("password", COUCHBASE_PASSWORD);
        options.put("bucket", COUCHBASE_BUCKET);
        options.put("scope", COUCHBASE_SOURCE_SCOPE);
        options.put("collection", COUCHBASE_TIMEOUT_COLLECTION);
        options.put("query.timeout", timeoutSeconds);
        options.put(
                "schema",
                Collections.singletonMap("fields", Collections.singletonMap("id", "int")));
        return new CouchbaseSourceFactory()
                .<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>createSource(
                        new TableSourceFactoryContext(
                                ReadonlyConfig.fromMap(options), getClass().getClassLoader()))
                .createSource()
                .createReader(context);
    }

    private Collector<SeaTunnelRow> collectorWithInitialDelay(List<Integer> ids, int delaySeconds) {
        return new Collector<SeaTunnelRow>() {
            private final Object lock = new Object();

            @Override
            public void collect(SeaTunnelRow row) {
                ids.add((Integer) row.getField(0));
                if (ids.size() == 1) {
                    // Stall only after the first row to distinguish streaming from startup timeout.
                    try {
                        TimeUnit.SECONDS.sleep(delaySeconds);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IllegalStateException("Slow collector interrupted", e);
                    }
                }
            }

            @Override
            public Object getCheckpointLock() {
                return lock;
            }
        };
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
