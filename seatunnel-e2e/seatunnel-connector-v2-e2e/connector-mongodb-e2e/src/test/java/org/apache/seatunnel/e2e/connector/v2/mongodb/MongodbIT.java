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
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
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
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Slf4j
public class MongodbIT extends TestSuiteBase implements TestResource {

    private static final String MONGODB_IMAGE = "mongo:latest";
    private static final String MONGODB_CONTAINER_HOST = "e2e_mongodb";
    private static final int MONGODB_PORT = 27017;
    private static final String MONGODB_DATABASE = "test_db";
    private static final String MONGODB_SOURCE_TABLE = "source_table";

    private static final Random random = new Random();

    private static final List<Document> TEST_DATASET = generateTestDataSet(5);

    private GenericContainer<?> mongodbContainer;
    private MongoClient client;

    @TestTemplate
    public void testMongodbSourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/mongodb_source_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    @TestTemplate
    public void testFakeSourceToMongodbSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/fake_source_to_mongodb.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void initConnection() {
        String host = mongodbContainer.getContainerIpAddress();
        int port = mongodbContainer.getFirstMappedPort();
        String url = String.format("mongodb://%s:%d/%s", host, port, MONGODB_DATABASE);
        client = MongoClients.create(url);
    }

    private void initSourceData(String database, String table, List<Document> documents) {
        MongoCollection<Document> sourceTable = client.getDatabase(database).getCollection(table);

        sourceTable.deleteMany(new Document());
        sourceTable.insertMany(documents);
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
                                            random.nextInt(),
                                            random.nextInt(),
                                            random.nextInt(),
                                            random.nextInt(),
                                            random.nextInt()))
                            .append("c_string", randomString())
                            .append("c_boolean", random.nextBoolean())
                            .append("c_tinyint", (byte) random.nextInt(256))
                            .append("c_smallint", (short) random.nextInt(65536))
                            .append("c_int", random.nextInt())
                            .append("c_bigint", random.nextLong())
                            .append("c_float", random.nextFloat() * Float.MAX_VALUE)
                            .append("c_double", random.nextDouble() * Double.MAX_VALUE)
                            .append("c_bytes", randomString().getBytes(StandardCharsets.UTF_8))
                            .append("c_date", new Date(random.nextLong())) // Random Date
                            .append("c_decimal", new BigDecimal(random.nextInt(1000000000)))
                            .append("c_timestamp", new Date(random.nextLong())) // Random Timestamp
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
                                                            random.nextInt(),
                                                            random.nextInt(),
                                                            random.nextInt(),
                                                            random.nextInt(),
                                                            random.nextInt()))
                                            .append("c_string", randomString())
                                            .append("c_boolean", random.nextBoolean())
                                            .append("c_tinyint", (byte) random.nextInt(256))
                                            .append("c_smallint", (short) random.nextInt(65536))
                                            .append("c_int", random.nextInt())
                                            .append("c_bigint", random.nextLong())
                                            .append("c_float", random.nextFloat() * Float.MAX_VALUE)
                                            .append(
                                                    "c_double",
                                                    random.nextDouble() * Double.MAX_VALUE)
                                            .append(
                                                    "c_bytes",
                                                    randomString().getBytes(StandardCharsets.UTF_8))
                                            .append(
                                                    "c_date",
                                                    new Date(random.nextLong())) // Random Date
                                            .append(
                                                    "c_decimal",
                                                    new BigDecimal(random.nextInt(1000000000)))
                                            .append("c_timestamp", new Date(random.nextLong())))
                            .append("id", i + 1));
        }

        return dataSet;
    }

    private static String randomString() {
        int length = random.nextInt(10) + 1;
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            char c = (char) (random.nextInt(26) + 'a');
            sb.append(c);
        }
        return sb.toString();
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
        Startables.deepStart(Stream.of(mongodbContainer)).join();
        log.info("Mongodb container started");

        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        this.initSourceData(MONGODB_DATABASE, MONGODB_SOURCE_TABLE, TEST_DATASET);
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
