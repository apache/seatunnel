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

import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static org.awaitility.Awaitility.await;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support cdc")
@Slf4j
public class MongodbCDCIT extends AbstractMongodbIT {

    @TestTemplate
    public void testMongodbCDCSink(TestContainer container) {
        CompletableFuture<Void> executeJobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                container.executeJob("/cdcIT/mysqlcdc_to_mongodb.conf");
                            } catch (Exception e) {
                                log.error("Commit task exception :" + e.getMessage());
                                throw new RuntimeException(e);
                            }
                            return null;
                        });

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<LinkedHashMap<String, Object>> expected = querySql(SOURCE_SQL);
                            List<Document> actual = readMongodbData(MONGODB_CDC_RESULT_TABLE);

                            List<LinkedHashMap<String, Object>> actualMapped =
                                    actual.stream()
                                            .map(LinkedHashMap::new)
                                            .peek(map -> map.remove("_id"))
                                            .collect(Collectors.toList());

                            Assertions.assertIterableEquals(expected, actualMapped);
                        });

        upsertDeleteSourceTable();
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            List<LinkedHashMap<String, Object>> expected = querySql(SOURCE_SQL);
                            List<Document> actual = readMongodbData(MONGODB_CDC_RESULT_TABLE);

                            List<LinkedHashMap<String, Object>> actualMapped =
                                    actual.stream()
                                            .map(LinkedHashMap::new)
                                            .peek(map -> map.remove("_id"))
                                            .collect(Collectors.toList());

                            Assertions.assertIterableEquals(expected, actualMapped);
                        });
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
        // Used for local testing
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

        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
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
