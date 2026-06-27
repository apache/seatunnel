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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Base64;
import java.util.List;
import java.util.stream.Stream;

/**
 * End-to-end integration test for the Couchbase sink connector.
 *
 * <p>Starts a real Couchbase community server via Testcontainers, writes data through SeaTunnel
 * using the FakeSource connector, and verifies that the documents appear in the target collection.
 */
@Slf4j
public class CouchbaseIT extends TestSuiteBase implements TestResource {

    private static final String COUCHBASE_IMAGE = "couchbase/server:community-7.1.1";
    private static final String COUCHBASE_CONTAINER_HOST = "e2e_couchbase";
    private static final int COUCHBASE_PORT = 8091;
    private static final int COUCHBASE_QUERY_PORT = 8093;
    private static final String COUCHBASE_USERNAME = "Administrator";
    private static final String COUCHBASE_PASSWORD = "password";
    private static final String COUCHBASE_BUCKET = "test_bucket";
    private static final String COUCHBASE_SCOPE = "_default";
    private static final String COUCHBASE_COLLECTION = "test_collection";

    private GenericContainer<?> couchbaseContainer;
    private Cluster cluster;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        couchbaseContainer =
                new GenericContainer<>(DockerImageName.parse(COUCHBASE_IMAGE))
                        .withNetworkAliases(COUCHBASE_CONTAINER_HOST)
                        .withExposedPorts(COUCHBASE_PORT, COUCHBASE_QUERY_PORT)
                        .withEnv("COUCHBASE_ADMINISTRATOR_USERNAME", COUCHBASE_USERNAME)
                        .withEnv("COUCHBASE_ADMINISTRATOR_PASSWORD", COUCHBASE_PASSWORD)
                        .waitingFor(Wait.forHttp("/ui/index.html").forPort(COUCHBASE_PORT))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(COUCHBASE_IMAGE)));

        Startables.deepStart(Stream.of(couchbaseContainer)).join();

        String host = couchbaseContainer.getHost();
        int managementPort = couchbaseContainer.getMappedPort(COUCHBASE_PORT);
        String baseUrl = "http://" + host + ":" + managementPort;
        String connectionString = "couchbase://" + host + ":" + managementPort;

        // Bootstrap the single-node cluster via REST.
        HttpClient http = HttpClient.newHttpClient();
        String auth =
                Base64.getEncoder()
                        .encodeToString((COUCHBASE_USERNAME + ":" + COUCHBASE_PASSWORD).getBytes());

        // 1. Configure node paths and services.
        httpPost(
                http,
                baseUrl + "/nodes/self/controller/settings",
                auth,
                "path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata"
                        + "&index_path=%2Fopt%2Fcouchbase%2Fvar%2Flib%2Fcouchbase%2Fdata");
        httpPost(http, baseUrl + "/pools/default", auth, "memoryQuota=512&indexMemoryQuota=256");
        httpPost(
                http,
                baseUrl + "/node/controller/setupServices",
                auth,
                "services=kv%2Cn1ql%2Cindex");
        httpPost(
                http,
                baseUrl + "/settings/web",
                auth,
                "port=8091&username=" + COUCHBASE_USERNAME + "&password=" + COUCHBASE_PASSWORD);

        // 2. Create bucket.
        httpPost(
                http,
                baseUrl + "/pools/default/buckets",
                auth,
                "name=" + COUCHBASE_BUCKET + "&ramQuota=256&bucketType=couchbase&replicaNumber=0");
        Thread.sleep(3000);

        // 3. Create collection.
        httpPost(
                http,
                baseUrl
                        + "/pools/default/buckets/"
                        + COUCHBASE_BUCKET
                        + "/scopes/_default/collections",
                auth,
                "name=" + COUCHBASE_COLLECTION);
        Thread.sleep(3000);

        // 4. Set index storage mode so primary-index creation works.
        httpPost(http, baseUrl + "/settings/indexes", auth, "storageMode=forestdb");

        cluster = Cluster.connect(connectionString, COUCHBASE_USERNAME, COUCHBASE_PASSWORD);
        cluster.bucket(COUCHBASE_BUCKET).waitUntilReady(Duration.ofSeconds(30));
        log.info("Couchbase container started, connection string: {}", connectionString);
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
     * Issues an HTTP POST with form-encoded body. Errors are logged as warnings — bootstrap steps
     * are best-effort.
     */
    private static void httpPost(HttpClient http, String url, String auth, String body)
            throws Exception {
        HttpRequest req =
                HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .header("Authorization", "Basic " + auth)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() >= 400) {
            // Log but don't fail — some steps may already be done (e.g. re-run).
            System.err.println("WARN: POST " + url + " -> HTTP " + resp.statusCode());
        }
    }

    /** Verifies that FakeSource data is correctly written to the Couchbase collection. */
    @TestTemplate
    public void testFakeSourceToCouchbaseSink(TestContainer container)
            throws IOException, InterruptedException {
        container.executeJob("/fake_source_to_couchbase.conf");

        // Query the collection to verify records were written.
        String query =
                String.format(
                        "SELECT COUNT(*) AS cnt FROM `%s`.`%s`.`%s`",
                        COUCHBASE_BUCKET, COUCHBASE_SCOPE, COUCHBASE_COLLECTION);
        QueryResult result = cluster.query(query);
        List<JsonObject> rows = result.rowsAs(JsonObject.class);

        Assertions.assertFalse(rows.isEmpty(), "Expected at least one row in the query result");
        int count = rows.get(0).getInt("cnt");
        Assertions.assertTrue(count > 0, "Expected documents to be written to Couchbase");
        log.info("Verified {} documents in Couchbase collection", count);
    }
}
