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

package org.apache.seatunnel.e2e.connector.amazondocumentdb;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class AmazonDocumentDBIT extends TestSuiteBase implements TestResource {

    private static final String MONGO_IMAGE = "mongo:latest";
    private static final String MONGO_HOST = "e2e_amazondocumentdb";
    private static final int MONGO_PORT = 27017;
    private static final String USERNAME = "seatunnel";
    private static final String PASSWORD = "seatunnel-password";
    private static final String DATABASE = "seatunnel_e2e";
    private static final String COLLECTION = "source_orders";

    private GenericContainer<?> mongoContainer;
    private MongoClient mongoClient;

    @BeforeAll
    @Override
    public void startUp() {
        DockerImageName imageName = DockerImageName.parse(MONGO_IMAGE);
        mongoContainer =
                new GenericContainer<>(imageName)
                        .withEnv("MONGO_INITDB_ROOT_USERNAME", USERNAME)
                        .withEnv("MONGO_INITDB_ROOT_PASSWORD", PASSWORD)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MONGO_HOST)
                        .withExposedPorts(MONGO_PORT)
                        .waitingFor(
                                Wait.forListeningPort().withStartupTimeout(Duration.ofMinutes(2)))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(MONGO_IMAGE)));
        Startables.deepStart(Stream.of(mongoContainer)).join();
        log.info("MongoDB compatibility container for Amazon DocumentDB started");

        Awaitility.given()
                .ignoreExceptions()
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::assertMongoAvailable);

        mongoClient = MongoClients.create(hostConnectionString());
        MongoCollection<BsonDocument> collection =
                mongoClient.getDatabase(DATABASE).getCollection(COLLECTION, BsonDocument.class);
        collection.insertMany(
                Arrays.asList(
                        order("1", "alpha", 10), order("2", "beta", 20), order("3", "gamma", 30)));
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (mongoClient != null) {
            mongoClient.close();
        }
        if (mongoContainer != null) {
            mongoContainer.close();
        }
    }

    @TestTemplate
    public void testBasicRead(TestContainer container) throws IOException, InterruptedException {
        assertJobSucceeded(container, "/amazondocumentdb_source_basic.conf");
    }

    @TestTemplate
    public void testQueryFilter(TestContainer container) throws IOException, InterruptedException {
        assertJobSucceeded(container, "/amazondocumentdb_source_query_filter.conf");
    }

    @TestTemplate
    public void testProjection(TestContainer container) throws IOException, InterruptedException {
        assertJobSucceeded(container, "/amazondocumentdb_source_projection.conf");
    }

    private void assertMongoAvailable() {
        try (MongoClient candidate = MongoClients.create(hostConnectionString())) {
            BsonDocument result =
                    candidate
                            .getDatabase("admin")
                            .runCommand(
                                    new BsonDocument("ping", new BsonInt32(1)), BsonDocument.class);
            Assertions.assertEquals(1, result.get("ok").asNumber().intValue());
        }
    }

    private String hostConnectionString() {
        return String.format(
                "mongodb://%s:%s@%s:%d/?authSource=admin&retryWrites=false",
                USERNAME,
                PASSWORD,
                mongoContainer.getHost(),
                mongoContainer.getMappedPort(MONGO_PORT));
    }

    private static BsonDocument order(String id, String name, int score) {
        return new BsonDocument()
                .append("id", new BsonString(id))
                .append("name", new BsonString(name))
                .append("score", new BsonInt32(score));
    }

    private static void assertJobSucceeded(TestContainer container, String configPath)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob(configPath);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
