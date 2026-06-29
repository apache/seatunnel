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
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.couchbase.BucketDefinition;
import org.testcontainers.couchbase.CouchbaseContainer;
import org.testcontainers.couchbase.CouchbaseService;
import org.testcontainers.utility.DockerImageName;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
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

    /** Matches row.num in fake_source_to_couchbase.conf. */
    private static final int EXPECTED_ROW_COUNT = 100;

    private CouchbaseContainer couchbaseContainer;
    private Cluster cluster;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
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
                                CouchbaseService.KV,
                                CouchbaseService.QUERY,
                                CouchbaseService.INDEX);
        couchbaseContainer.start();

        cluster =
                Cluster.connect(
                        couchbaseContainer.getConnectionString(),
                        couchbaseContainer.getUsername(),
                        couchbaseContainer.getPassword());

        // Create the scoped collection and a primary index on it for N1QL queries.
        cluster.query(
                "CREATE COLLECTION `"
                        + COUCHBASE_BUCKET
                        + "`.`"
                        + COUCHBASE_SCOPE
                        + "`.`"
                        + COUCHBASE_COLLECTION
                        + "`");
        cluster.query(
                "CREATE PRIMARY INDEX ON `"
                        + COUCHBASE_BUCKET
                        + "`.`"
                        + COUCHBASE_SCOPE
                        + "`.`"
                        + COUCHBASE_COLLECTION
                        + "`");

        // Wait until the primary index is online before returning. The index build is
        // asynchronous; without this guard, the first N1QL query in the test body may
        // hit the collection before the index is ready and return an empty result set.
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

        log.info("Couchbase cluster ready at {}", couchbaseContainer.getConnectionString());
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (cluster != null) {
            cluster.disconnect();
        }
        if (couchbaseContainer != null) {
            couchbaseContainer.stop();
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
        // Do not filter by a specific id value — FakeSource generates random integers so
        // id=0 is not guaranteed to exist. Pick any document and verify field presence.
        String contentQuery =
                String.format(
                        "SELECT id, name, score, `active` FROM `%s`.`%s`.`%s` LIMIT 1",
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

        // --- Assertion 2: content check on the document keyed by id=0 ---
        // primary-key=["id"] in conf, so the document key is "0"
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
                            // id=0 is a valid integer; containsKey is safe where getInt() is not.
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
}
