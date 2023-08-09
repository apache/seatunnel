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
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support cdc")
public class MongodbCDCIT extends TestSuiteBase implements TestResource {

    // ----------------------------------------------------------------------------
    // mongodb
    protected static final String MONGODB_DATABASE = "inventory";

    protected static final String MONGODB_COLLECTION = "products";
    protected MongoDBContainer mongodbContainer;

    protected MongoClient client;

    // ----------------------------------------------------------------------------
    // mysql
    private static final String MYSQL_HOST = "mysql_e2e";

    private static final String MYSQL_USER_NAME = "st_user";

    private static final String MYSQL_USER_PASSWORD = "seatunnel";

    private static final String MYSQL_DATABASE = "mongodb_cdc";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer();

    // mysql sink table query sql
    private static final String SINK_SQL = "select name,description,weight from products";

    private static final String MYSQL_DRIVER_JAR =
            "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.16/mysql-connector-java-8.0.16.jar";

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer() {
        MySqlContainer mySqlContainer = new MySqlContainer(MySqlVersion.V8_0);
        mySqlContainer.withNetwork(NETWORK);
        mySqlContainer.withNetworkAliases(MYSQL_HOST);
        mySqlContainer.withDatabaseName(MYSQL_DATABASE);
        mySqlContainer.withUsername(MYSQL_USER_NAME);
        mySqlContainer.withPassword(MYSQL_USER_PASSWORD);
        mySqlContainer.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("Mysql-Docker-Image")));
        // For local test use
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
    public void startUp() {
        log.info("The first stage:Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        inventoryDatabase.createAndInitialize();
        log.info("Mysql ddl-a execution is complete");

        log.info("The second stage:Starting Mongodb containers...");
        mongodbContainer = new MongoDBContainer(NETWORK);
        // For local test use
        mongodbContainer.setPortBindings(Collections.singletonList("27017:27017"));
        mongodbContainer.withLogConsumer(
                new Slf4jLogConsumer(DockerLoggerFactory.getLogger("Mongodb-Docker-Image")));

        Startables.deepStart(Stream.of(mongodbContainer)).join();
        mongodbContainer.executeCommandFileInSeparateDatabase(MONGODB_DATABASE);
        initConnection();
        log.info("Mongodb Container are started");
    }

    @TestTemplate
    public void testMongodbCdcToMysqlCheckDataE2e(TestContainer container) {
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/mongodbcdc_to_mysql.conf");
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException();
                    }
                    return null;
                });
        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    readMongodbData().stream()
                                            .peek(e -> e.remove("_id"))
                                            .map(Document::entrySet)
                                            .map(Set::stream)
                                            .map(
                                                    entryStream ->
                                                            entryStream
                                                                    .map(Map.Entry::getValue)
                                                                    .collect(
                                                                            Collectors.toCollection(
                                                                                    ArrayList
                                                                                            ::new)))
                                            .collect(Collectors.toList()),
                                    querySql());
                        });

        // insert update delete
        upsertDeleteSourceTable();

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    readMongodbData().stream()
                                            .peek(e -> e.remove("_id"))
                                            .map(Document::entrySet)
                                            .map(Set::stream)
                                            .map(
                                                    entryStream ->
                                                            entryStream
                                                                    .map(Map.Entry::getValue)
                                                                    .collect(
                                                                            Collectors.toCollection(
                                                                                    ArrayList
                                                                                            ::new)))
                                            .collect(Collectors.toList()),
                                    querySql());
                        });
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> querySql() {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(MongodbCDCIT.SINK_SQL);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.info("Print mysql sink data:" + objects);
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void upsertDeleteSourceTable() {
        mongodbContainer.executeCommandFileInDatabase("inventoryDDL", MONGODB_DATABASE);
    }

    public void initConnection() {
        String ipAddress = mongodbContainer.getHost();
        Integer port = mongodbContainer.getFirstMappedPort();
        String url =
                String.format(
                        "mongodb://%s:%s@%s:%d/%s?authSource=admin",
                        "superuser",
                        "superpw",
                        ipAddress,
                        port,
                        MONGODB_DATABASE + "." + MONGODB_COLLECTION);
        client = MongoClients.create(url);
    }

    protected List<Document> readMongodbData() {
        MongoCollection<Document> sinkTable =
                client.getDatabase(MONGODB_DATABASE).getCollection(MongodbCDCIT.MONGODB_COLLECTION);
        // If the cursor has been traversed, it will automatically close without explicitly closing.
        MongoCursor<Document> cursor = sinkTable.find().sort(Sorts.ascending("_id")).cursor();
        List<Document> documents = new ArrayList<>();
        while (cursor.hasNext()) {
            documents.add(cursor.next());
        }
        return documents;
    }

    @AfterAll
    @Override
    public void tearDown() {
        // close Container
        if (Objects.nonNull(client)) {
            client.close();
        }
        MYSQL_CONTAINER.close();
        if (mongodbContainer != null) {
            mongodbContainer.close();
        }
    }
}
