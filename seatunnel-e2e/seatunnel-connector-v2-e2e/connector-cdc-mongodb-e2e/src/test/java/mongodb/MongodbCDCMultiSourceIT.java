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
package mongodb;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class MongodbCDCMultiSourceIT extends TestSuiteBase implements TestResource {

    private static final Logger LOG = LoggerFactory.getLogger(MongodbCDCMultiSourceIT.class);

    private static final Network NETWORK = Network.newNetwork();

    protected static final String MONGODB_DATABASE_A = "inventory_a";
    protected static final String MONGODB_COLLECTION_A = "products_a";
    protected MongoDBContainer mongodbContainerA;
    protected MongoClient clientA;

    protected static final String MONGODB_DATABASE_B = "inventory_b";
    protected static final String MONGODB_COLLECTION_B = "products_b";
    protected MongoDBContainer mongodbContainerB;
    protected MongoClient clientB;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        mongodbContainerA =
                new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.SHARD);
        mongodbContainerA.setPortBindings(Collections.singletonList("27017:27017"));
        mongodbContainerA.withLogConsumer(
                new org.testcontainers.containers.output.Slf4jLogConsumer(
                        DockerLoggerFactory.getLogger("MongoDB-A-Docker-Image")));
        Startables.deepStart(Stream.of(mongodbContainerA)).join();

        mongodbContainerB =
                new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.SHARD);
        mongodbContainerB.setPortBindings(Collections.singletonList("27018:27017"));
        mongodbContainerB.withLogConsumer(
                new org.testcontainers.containers.output.Slf4jLogConsumer(
                        DockerLoggerFactory.getLogger("MongoDB-B-Docker-Image")));
        Startables.deepStart(Stream.of(mongodbContainerB)).join();

        initMongoDBConnections();
        initMongoDBData();
    }

    private void initMongoDBConnections() {
        String ipAddressA = mongodbContainerA.getHost();
        Integer portA = mongodbContainerA.getFirstMappedPort();
        String urlA =
                String.format(
                        "mongodb://%s:%s@%s:%d/%s?authSource=admin",
                        "superuser", "superpw", ipAddressA, portA, MONGODB_DATABASE_A);
        clientA = MongoClients.create(urlA);
        log.info("Connected to MongoDB A at {}:{}", ipAddressA, portA);

        String ipAddressB = mongodbContainerB.getHost();
        Integer portB = mongodbContainerB.getFirstMappedPort();
        String urlB =
                String.format(
                        "mongodb://%s:%s@%s:%d/%s?authSource=admin",
                        "superuser", "superpw", ipAddressB, portB, MONGODB_DATABASE_B);
        clientB = MongoClients.create(urlB);
        log.info("Connected to MongoDB B at {}:{}", ipAddressB, portB);
    }

    private void initMongoDBData() {
        MongoCollection<Document> collectionA =
                clientA.getDatabase(MONGODB_DATABASE_A).getCollection(MONGODB_COLLECTION_A);
        collectionA.deleteMany(new Document());
        List<Document> dataA = new ArrayList<>();
        dataA.add(new Document("_id", "A001").append("name", "Product A1").append("price", 100));
        dataA.add(new Document("_id", "A002").append("name", "Product A2").append("price", 200));
        dataA.add(new Document("_id", "A003").append("name", "Product A3").append("price", 300));
        collectionA.insertMany(dataA);
        log.info("Inserted {} documents into MongoDB A", dataA.size());

        MongoCollection<Document> collectionB =
                clientB.getDatabase(MONGODB_DATABASE_B).getCollection(MONGODB_COLLECTION_B);
        collectionB.deleteMany(new Document());
        List<Document> dataB = new ArrayList<>();
        dataB.add(new Document("_id", "B001").append("name", "Product B1").append("price", 150));
        dataB.add(new Document("_id", "B002").append("name", "Product B2").append("price", 250));
        dataB.add(new Document("_id", "B003").append("name", "Product B3").append("price", 350));
        collectionB.insertMany(dataB);
        log.info("Inserted {} documents into MongoDB B", dataB.size());
    }

    @TestTemplate
    public void testMultipleMongoDBSourcesSequentially(TestContainer container) throws Exception {

        Container.ExecResult mongoAResult = container.executeJob("/mongodb_multi_source_a.conf");
        Assertions.assertEquals(0, mongoAResult.getExitCode());

        Container.ExecResult mongoBResult = container.executeJob("/mongodb_multi_source_b.conf");
        Assertions.assertEquals(0, mongoBResult.getExitCode());
    }

    @AfterAll
    @Override
    public void tearDown() {

        if (clientA != null) {
            clientA.close();
        }

        if (clientB != null) {
            clientB.close();
        }

        if (mongodbContainerA != null) {
            mongodbContainerA.stop();
        }

        if (mongodbContainerB != null) {
            mongodbContainerB.stop();
        }
    }
}
