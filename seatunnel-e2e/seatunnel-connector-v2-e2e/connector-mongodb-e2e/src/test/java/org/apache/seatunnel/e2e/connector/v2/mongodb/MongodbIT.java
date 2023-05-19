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

package org.apache.seatunnel.e2e.connector.v2.mongodb;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Slf4j
public class MongodbIT extends TestSuiteBase implements TestResource {

    private static final Random RANDOM = new Random();

    private static final List<Document> TEST_MATCH_DATASET = generateTestDataSet(5);

    private static final List<Document> TEST_SPLIT_DATASET = generateTestDataSet(10);

    private static final String MONGODB_IMAGE = "mongo:latest";

    private static final String MONGODB_CONTAINER_HOST = "e2e_mongodb";

    private static final int MONGODB_PORT = 27017;

    private static final String MONGODB_DATABASE = "test_db";

    private static final String MONGODB_MATCH_TABLE = "test_match_op_db";

    private static final String MONGODB_SPLIT_TABLE = "test_split_op_db";

    private static final String MONGODB_MATCH_RESULT_TABLE = "test_match_op_result_db";

    private static final String MONGODB_SPLIT_RESULT_TABLE = "test_split_op_result_db";

    private static final String MONGODB_SINK_TABLE = "test_source_sink_table";

    private static final String MONGODB_UPDATE_TABLE = "test_update_table";

    private static final String MONGODB_FLAT_TABLE = "test_flat_table";

    private GenericContainer<?> mongodbContainer;

    private MongoClient client;

    @TestTemplate
    public void testMongodbSourceAndSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult insertResult = container.executeJob("/fake_source_to_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult assertResult = container.executeJob("/mongodb_source_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());
        clearDate(MONGODB_SINK_TABLE);
    }

    @TestTemplate
    public void testMongodbSourceMatch(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult queryResult =
                container.executeJob("/matchIT/mongodb_matchQuery_source_to_assert.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_MATCH_DATASET.stream()
                        .filter(x -> x.get("c_int").equals(2))
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_MATCH_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearDate(MONGODB_MATCH_RESULT_TABLE);

        Container.ExecResult projectionResult =
                container.executeJob("/matchIT/mongodb_matchProjection_source_to_assert.conf");
        Assertions.assertEquals(0, projectionResult.getExitCode(), projectionResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_MATCH_DATASET.stream()
                        .map(Document::new)
                        .peek(document -> document.remove("c_bigint"))
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_MATCH_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearDate(MONGODB_MATCH_RESULT_TABLE);
    }

    @TestTemplate
    public void testFakeSourceToUpdateMongodb(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult insertResult =
                container.executeJob("/updateIT/fake_source_to_updateMode_insert_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult updateResult =
                container.executeJob("/updateIT/fake_source_to_update_mongodb.conf");
        Assertions.assertEquals(0, updateResult.getExitCode(), updateResult.getStderr());

        Container.ExecResult assertResult =
                container.executeJob("/updateIT/update_mongodb_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());

        clearDate(MONGODB_UPDATE_TABLE);
    }

    @TestTemplate
    public void testFlatSyncString(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult insertResult =
                container.executeJob("/flatIT/fake_source_to_flat_mongodb.conf");
        Assertions.assertEquals(0, insertResult.getExitCode(), insertResult.getStderr());

        Container.ExecResult assertResult =
                container.executeJob("/flatIT/mongodb_flat_source_to_assert.conf");
        Assertions.assertEquals(0, assertResult.getExitCode(), assertResult.getStderr());

        clearDate(MONGODB_FLAT_TABLE);
    }

    @TestTemplate
    public void testMongodbSourceSplit(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult queryResult =
                container.executeJob("/splitIT/mongodb_split_key_source_to_assert.conf");
        Assertions.assertEquals(0, queryResult.getExitCode(), queryResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_SPLIT_DATASET.stream()
                        .map(Document::new)
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_SPLIT_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearDate(MONGODB_SPLIT_RESULT_TABLE);

        Container.ExecResult projectionResult =
                container.executeJob("/splitIT/mongodb_split_size_source_to_assert.conf");
        Assertions.assertEquals(0, projectionResult.getExitCode(), projectionResult.getStderr());

        Assertions.assertIterableEquals(
                TEST_SPLIT_DATASET.stream()
                        .map(Document::new)
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()),
                readMongodbData(MONGODB_SPLIT_RESULT_TABLE).stream()
                        .peek(e -> e.remove("_id"))
                        .collect(Collectors.toList()));
        clearDate(MONGODB_SPLIT_RESULT_TABLE);
    }

    public void initConnection() {
        String host = mongodbContainer.getContainerIpAddress();
        int port = mongodbContainer.getFirstMappedPort();
        String url = String.format("mongodb://%s:%d/%s", host, port, MONGODB_DATABASE);
        client = MongoClients.create(url);
    }

    private void initSourceData() {
        MongoCollection<Document> sourceMatchTable =
                client.getDatabase(MongodbIT.MONGODB_DATABASE)
                        .getCollection(MongodbIT.MONGODB_MATCH_TABLE);

        sourceMatchTable.deleteMany(new Document());
        sourceMatchTable.insertMany(MongodbIT.TEST_MATCH_DATASET);

        MongoCollection<Document> sourceSplitTable =
                client.getDatabase(MongodbIT.MONGODB_DATABASE)
                        .getCollection(MongodbIT.MONGODB_SPLIT_TABLE);

        sourceSplitTable.deleteMany(new Document());
        sourceSplitTable.insertMany(MongodbIT.TEST_SPLIT_DATASET);
    }

    private void clearDate(String table) {
        client.getDatabase(MONGODB_DATABASE).getCollection(table).drop();
    }

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName imageName = DockerImageName.parse(MONGODB_IMAGE);
        mongodbContainer =
                new GenericContainer<>(imageName)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MONGODB_CONTAINER_HOST)
                        .withExposedPorts(MONGODB_PORT)
                        .waitingFor(
                                new HttpWaitStrategy()
                                        .forPort(MONGODB_PORT)
                                        .forStatusCodeMatching(
                                                response ->
                                                        response == HTTP_OK
                                                                || response == HTTP_UNAUTHORIZED)
                                        .withStartupTimeout(Duration.ofMinutes(2)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MONGODB_IMAGE)));
        mongodbContainer.setPortBindings(Collections.singletonList("27017:27017"));
        Startables.deepStart(Stream.of(mongodbContainer)).join();
        log.info("Mongodb container started");

        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        this.initSourceData();
    }

    public static List<Document> generateTestDataSet(int count) {
        List<Document> dataSet = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            dataSet.add(
                    new Document(
                                    "c_map",
                                    new Document("OQBqH", randomString())
                                            .append("rkvlO", randomString())
                                            .append("pCMEX", randomString())
                                            .append("DAgdj", randomString())
                                            .append("dsJag", randomString()))
                            .append(
                                    "c_array",
                                    Arrays.asList(
                                            RANDOM.nextInt(),
                                            RANDOM.nextInt(),
                                            RANDOM.nextInt(),
                                            RANDOM.nextInt(),
                                            RANDOM.nextInt()))
                            .append("c_string", randomString())
                            .append("c_boolean", RANDOM.nextBoolean())
                            .append("c_int", i)
                            .append("c_bigint", RANDOM.nextLong())
                            .append("c_double", RANDOM.nextDouble() * Double.MAX_VALUE)
                            .append(
                                    "c_row",
                                    new Document(
                                                    "c_map",
                                                    new Document("OQBqH", randomString())
                                                            .append("rkvlO", randomString())
                                                            .append("pCMEX", randomString())
                                                            .append("DAgdj", randomString())
                                                            .append("dsJag", randomString()))
                                            .append(
                                                    "c_array",
                                                    Arrays.asList(
                                                            RANDOM.nextInt(),
                                                            RANDOM.nextInt(),
                                                            RANDOM.nextInt(),
                                                            RANDOM.nextInt(),
                                                            RANDOM.nextInt()))
                                            .append("c_string", randomString())
                                            .append("c_boolean", RANDOM.nextBoolean())
                                            .append("c_int", RANDOM.nextInt())
                                            .append("c_bigint", RANDOM.nextLong())
                                            .append(
                                                    "c_double",
                                                    RANDOM.nextDouble() * Double.MAX_VALUE)));
        }
        return dataSet;
    }

    private static String randomString() {
        int length = RANDOM.nextInt(10) + 1;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = (char) (RANDOM.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
    }

    private List<Document> readMongodbData(String collection) {
        MongoCollection<Document> sinkTable =
                client.getDatabase(MONGODB_DATABASE).getCollection(collection);
        MongoCursor<Document> cursor = sinkTable.find().sort(Sorts.ascending("c_int")).cursor();
        List<Document> documents = new ArrayList<>();
        while (cursor.hasNext()) {
            documents.add(cursor.next());
        }
        return documents;
    }

    @Override
    public void tearDown() {
        if (client != null) {
            client.close();
        }
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
    }
}
