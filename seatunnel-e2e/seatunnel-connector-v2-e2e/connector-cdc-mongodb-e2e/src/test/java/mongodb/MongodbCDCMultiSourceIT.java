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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.testcontainers.shaded.org.awaitility.Awaitility.with;
import static org.testcontainers.shaded.org.awaitility.Durations.TWO_SECONDS;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class MongodbCDCMultiSourceIT extends TestSuiteBase implements TestResource {

    protected static final String MONGODB_DATABASE_A = "inventory_a";
    protected static final String MONGODB_COLLECTION_A = "products_a";
    protected MongoDBContainer mongodbContainerA;
    protected MongoClient clientA;

    protected static final String MONGODB_DATABASE_B = "inventory_b";
    protected static final String MONGODB_COLLECTION_B = "products_b";
    protected MongoDBContainer mongodbContainerB;
    protected MongoClient clientB;

    private static final String MYSQL_HOST = "mysql_e2e";
    private static final String MYSQL_USER_NAME = "st_user";
    private static final String MYSQL_USER_PASSWORD = "seatunnel";
    private static final String MYSQL_DATABASE = "mongodb_cdc";
    private static final String MYSQL_DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();
    private final UniqueDatabase database = new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer() {
        MySqlContainer mySqlContainer = new MySqlContainer(MySqlVersion.V8_0);
        mySqlContainer.withNetwork(NETWORK);
        mySqlContainer.withNetworkAliases(MYSQL_HOST);
        mySqlContainer.withDatabaseName(MYSQL_DATABASE);
        mySqlContainer.withUsername(MYSQL_USER_NAME);
        mySqlContainer.withPassword(MYSQL_USER_PASSWORD);
        mySqlContainer.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("Mysql-Docker-Image")));
        mySqlContainer.setPortBindings(Collections.singletonList("3310:3306"));
        return mySqlContainer;
    }

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + MYSQL_DRIVER_JAR);
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("Starting MySQL container...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("MySQL container started");
        database.createAndInitialize();
        log.info("MySQL database initialized");

        log.info("Starting MongoDB A container...");
        mongodbContainerA =
                new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.SHARD);
        mongodbContainerA.withNetworkAliases("mongo0");
        mongodbContainerA.setPortBindings(Collections.singletonList("27017:27017"));
        mongodbContainerA.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("MongoDB-A-Docker-Image")));
        Startables.deepStart(Stream.of(mongodbContainerA)).join();
        log.info("MongoDB A container started");

        log.info("Starting MongoDB B container...");
        mongodbContainerB =
                new MongoDBContainer(NETWORK, MongoDBContainer.ShardingClusterRole.SHARD);
        mongodbContainerB.withNetworkAliases("mongo1");
        mongodbContainerB.setPortBindings(Collections.singletonList("27018:27017"));
        mongodbContainerB.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("MongoDB-B-Docker-Image")));
        Startables.deepStart(Stream.of(mongodbContainerB)).join();
        log.info("MongoDB B container started");

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
        createMySqlTables();
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mongodb_multi_source_a.conf");
                    } catch (Exception e) {
                        log.error("MongoDB A job exception: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        assertMySqlHasData("products_a", 3);
        log.info("MongoDB A data verified in MySQL");

        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mongodb_multi_source_b.conf");
                    } catch (Exception e) {
                        log.error("MongoDB B job exception: " + e.getMessage());
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        assertMySqlHasData("products_b", 3);
        log.info("MongoDB B data verified in MySQL");
    }

    private void createMySqlTables() throws SQLException {
        try (Connection connection = getJdbcConnection()) {
            String createTableA =
                    "CREATE TABLE IF NOT EXISTS products_a ("
                            + "_id VARCHAR(255) PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "price INT"
                            + ")";
            connection.createStatement().execute(createTableA);
            log.info("Created table products_a");

            String createTableB =
                    "CREATE TABLE IF NOT EXISTS products_b ("
                            + "_id VARCHAR(255) PRIMARY KEY, "
                            + "name VARCHAR(255), "
                            + "price INT"
                            + ")";
            connection.createStatement().execute(createTableB);
            log.info("Created table products_b");
        }
    }

    private void assertMySqlHasData(String tableName, int expectedCount) {
        with().pollInterval(TWO_SECONDS)
                .pollDelay(500, TimeUnit.MILLISECONDS)
                .await()
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> {
                            try (Connection connection = getJdbcConnection()) {
                                String sql = String.format("SELECT COUNT(*) FROM %s", tableName);
                                try (ResultSet rs =
                                        connection.createStatement().executeQuery(sql)) {
                                    if (rs.next()) {
                                        int count = rs.getInt(1);
                                        log.info("Table {} has {} rows", tableName, count);
                                        Assertions.assertEquals(
                                                expectedCount,
                                                count,
                                                String.format(
                                                        "Expected %d rows in %s but found %d",
                                                        expectedCount, tableName, count));
                                    }
                                }
                            } catch (SQLException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
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

        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }
}
