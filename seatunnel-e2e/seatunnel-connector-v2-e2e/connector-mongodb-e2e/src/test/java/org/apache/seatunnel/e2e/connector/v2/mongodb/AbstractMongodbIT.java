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

import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Slf4j
public abstract class AbstractMongodbIT extends TestSuiteBase implements TestResource {

    protected static final Random RANDOM = new Random();

    protected static final List<Document> TEST_MATCH_DATASET = generateTestDataSet(5);

    protected static final List<Document> TEST_SPLIT_DATASET = generateTestDataSet(10);

    protected static final String MONGODB_IMAGE = "mongo:latest";

    protected static final String MONGODB_CONTAINER_HOST = "e2e_mongodb";

    protected static final int MONGODB_PORT = 27017;

    protected static final String MONGODB_DATABASE = "test_db";

    protected static final String MONGODB_MATCH_TABLE = "test_match_op_db";

    protected static final String MONGODB_SPLIT_TABLE = "test_split_op_db";

    protected static final String MONGODB_MATCH_RESULT_TABLE = "test_match_op_result_db";

    protected static final String MONGODB_SPLIT_RESULT_TABLE = "test_split_op_result_db";

    protected static final String MONGODB_SINK_TABLE = "test_source_sink_table";

    protected static final String MONGODB_UPDATE_TABLE = "test_update_table";

    protected static final String MONGODB_FLAT_TABLE = "test_flat_table";

    protected static final String MONGODB_CDC_RESULT_TABLE = "test_cdc_table";

    protected static final String MONGODB_TRANSACTION_SINK_TABLE =
            "test_source_transaction_sink_table";
    protected static final String MONGODB_TRANSACTION_UPSERT_TABLE =
            "test_source_upsert_transaction_table";

    protected GenericContainer<?> mongodbContainer;

    protected MongoClient client;

    public void initConnection() {
        String host = mongodbContainer.getContainerIpAddress();
        int port = mongodbContainer.getFirstMappedPort();
        String url = String.format("mongodb://%s:%d/%s", host, port, MONGODB_DATABASE);
        client = MongoClients.create(url);
    }

    protected void initSourceData() {
        MongoCollection<Document> sourceMatchTable =
                client.getDatabase(MONGODB_DATABASE).getCollection(MONGODB_MATCH_TABLE);

        sourceMatchTable.deleteMany(new Document());
        sourceMatchTable.insertMany(TEST_MATCH_DATASET);

        MongoCollection<Document> sourceSplitTable =
                client.getDatabase(MONGODB_DATABASE).getCollection(MONGODB_SPLIT_TABLE);

        sourceSplitTable.deleteMany(new Document());
        sourceSplitTable.insertMany(TEST_SPLIT_DATASET);
    }

    protected void clearDate(String table) {
        client.getDatabase(MONGODB_DATABASE).getCollection(table).drop();
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

    protected static String randomString() {
        int length = RANDOM.nextInt(10) + 1;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = (char) (RANDOM.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
    }

    protected List<Document> readMongodbData(String collection) {
        MongoCollection<Document> sinkTable =
                client.getDatabase(MONGODB_DATABASE).getCollection(collection);
        MongoCursor<Document> cursor = sinkTable.find().sort(Sorts.ascending("c_int")).cursor();
        List<Document> documents = new ArrayList<>();
        while (cursor.hasNext()) {
            documents.add(cursor.next());
        }
        return documents;
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
        // For local test use
        // mongodbContainer.setPortBindings(Collections.singletonList("27017:27017"));
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

    @AfterAll
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
