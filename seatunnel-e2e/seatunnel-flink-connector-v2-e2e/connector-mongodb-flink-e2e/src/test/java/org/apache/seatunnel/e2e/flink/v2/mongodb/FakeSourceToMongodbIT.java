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

package org.apache.seatunnel.e2e.flink.v2.mongodb;

import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;

import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCursor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.HttpWaitStrategy;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class FakeSourceToMongodbIT extends FlinkContainer {

    private static final String MONGODB_IMAGE = "mongo:latest";

    private static final String MONGODB_CONTAINER_HOST = "flink_e2e_mongodb_sink";

    private static final String MONGODB_HOST = "localhost";

    private static final int MONGODB_PORT = 27017;

    private static final String MONGODB_DATABASE = "test_db";

    private static final String MONGODB_COLLECTION = "test_table";

    private static final String MONGODB_URI = String.format("mongodb://%s:%d/%s", MONGODB_HOST, MONGODB_PORT, MONGODB_DATABASE);

    private MongoClient client;

    private GenericContainer<?> mongodbContainer;

    @BeforeEach
    public void startMongoContainer() {
        DockerImageName imageName = DockerImageName.parse(MONGODB_IMAGE);
        mongodbContainer = new GenericContainer<>(imageName)
            .withNetwork(NETWORK)
            .withNetworkAliases(MONGODB_CONTAINER_HOST)
            .withExposedPorts(MONGODB_PORT)
            .waitingFor(new HttpWaitStrategy()
                .forPort(MONGODB_PORT)
                .forStatusCodeMatching(response -> response == HTTP_OK || response == HTTP_UNAUTHORIZED)
                .withStartupTimeout(Duration.ofMinutes(2)))
            .withLogConsumer(new Slf4jLogConsumer(log));
        mongodbContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", MONGODB_PORT, MONGODB_PORT)));
        Startables.deepStart(Stream.of(mongodbContainer)).join();
        log.info("Mongodb container started");
        Awaitility.given().ignoreExceptions()
            .await()
            .atMost(30, TimeUnit.SECONDS)
            .untilAsserted(this::initConnection);
    }

    public void initConnection() {
        client = MongoClients.create(MONGODB_URI);
    }

    @Test
    public void testMongodbSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/mongodb/fake_to_mongodb.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        List<Map<String, Object>> list = new ArrayList<>();
        try (MongoCursor<Document> mongoCursor = client.getDatabase(MONGODB_DATABASE)
            .getCollection(MONGODB_COLLECTION)
            .find()
            .iterator()
        ) {
            while (mongoCursor.hasNext()) {
                Document doc = mongoCursor.next();
                HashMap<String, Object> map = new HashMap<>(doc.size());
                Set<Map.Entry<String, Object>> entries = doc.entrySet();
                for (Map.Entry<String, Object> entry : entries) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    map.put(key, value);
                }
                log.info("Document ===>>>: " + map);
                list.add(map);
            }
        }

        Assertions.assertEquals(10, list.size());
    }

    @AfterEach
    public void close() {
        if (client != null) {
            client.close();
        }
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
    }
}
