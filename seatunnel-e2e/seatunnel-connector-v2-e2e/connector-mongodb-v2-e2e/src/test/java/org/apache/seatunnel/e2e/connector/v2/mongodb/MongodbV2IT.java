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
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK, EngineType.SPARK})
public class MongodbV2IT extends TestSuiteBase implements TestResource {

    private static final String MONGODB_IMAGE = "mongo:latest";
    private static final String MONGODB_CONTAINER_HOST = "e2e_mongodb";
    private static final int MONGODB_PORT = 27017;
    private static final String MONGODB_DATABASE = "test_db";
    private static final String MONGODB_SOURCE_TABLE = "source_table";

    private static final Document TEST_DATASET = generateTestDataSet();

    private GenericContainer<?> mongodbContainer;
    private MongoClient client;

    @TestTemplate
    public void testMongodbSourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/mongodbV2_source_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    public void initConnection() {
        String host = mongodbContainer.getContainerIpAddress();
        int port = mongodbContainer.getFirstMappedPort();
        String url = String.format("mongodb://%s:%d/%s", host, port, MONGODB_DATABASE);
        client = MongoClients.create(url);
    }

    private void initSourceData(String database, String table, Document documents) {
        MongoCollection<Document> sourceTable = client.getDatabase(database).getCollection(table);

        sourceTable.deleteMany(new Document());
        sourceTable.insertOne(documents);
    }

    private static Document generateTestDataSet() {
        return new Document(
                        "c_map",
                        new Document("OQBqH", "wTKAH")
                                .append("rkvlO", "KXStv")
                                .append("pCMEX", "CyJKx")
                                .append("DAgdj", "SMbQe")
                                .append("dsJag", "jyFsb"))
                .append(
                        "c_array",
                        Arrays.asList(2095115245, 220036717, 1427565674, 454707262, 1254213323))
                .append("c_string", "rDAya")
                .append("c_boolean", true)
                .append("c_tinyint", (byte) 25)
                .append("c_smallint", (short) 22478)
                .append("c_int", 1333226130)
                .append("c_bigint", 2121370000000000000L)
                .append("c_float", 3.26072E+38f)
                .append("c_double", 9.9812E+307d)
                .append("c_bytes", "M0tZdnd3".getBytes(StandardCharsets.UTF_8))
                .append("c_date", new Date(1655097600000L)) // 2023-06-13
                .append("c_decimal", new BigDecimal("61746461279068200000"))
                .append("c_timestamp", new Date(1652770572000L)) // 2023-05-17 00:36:12
                .append(
                        "c_row",
                        new Document("c_map",
                                        new Document("OQBqH", "wTKAH")
                                                .append("rkvlO", "KXStv")
                                                .append("pCMEX", "CyJKx")
                                                .append("DAgdj", "SMbQe")
                                                .append("dsJag", "jyFsb"))
                                .append(
                                        "c_array",
                                        Arrays.asList(
                                                2095115245,
                                                220036717,
                                                1427565674,
                                                454707262,
                                                1254213323))
                                .append("c_string", "rDAya")
                                .append("c_boolean", true)
                                .append("c_tinyint", (byte) 25)
                                .append("c_smallint", (short) 22478)
                                .append("c_int", 1333226130)
                                .append("c_bigint", 2121370000000000000L)
                                .append("c_float", 3.26072E+38f)
                                .append("c_double", 9.9812E+307d)
                                .append("c_bytes", "M0tZdnd3".getBytes(StandardCharsets.UTF_8))
                                .append("c_date", new Date(1655097600000L)) // 2023-06-13
                                .append("c_decimal", new BigDecimal("61746461279068200000"))
                                .append("c_timestamp", new Date(1652770572000L)));
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
