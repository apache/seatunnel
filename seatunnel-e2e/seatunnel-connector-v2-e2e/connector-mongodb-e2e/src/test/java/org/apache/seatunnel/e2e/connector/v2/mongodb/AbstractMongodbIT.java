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

import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.bson.Document;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
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
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Random;

@Slf4j
public abstract class AbstractMongodbIT extends TestSuiteBase implements TestResource {

    protected static final String MYSQL_HOST = "mysql_cdc_e2e";

    protected static final String MYSQL_USER_NAME = "st_user";

    protected static final String MYSQL_USER_PASSWORD = "seatunnel";

    protected static final String MYSQL_DATABASE = "mysql_cdc";

    protected static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

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

    protected static final String SOURCE_SQL = "select * from products";

    protected final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw");

    protected GenericContainer<?> mongodbContainer;

    protected MongoClient client;

    protected void initConnection() {
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

    protected static MySqlContainer createMySqlContainer(MySqlVersion version) {
        MySqlContainer mySqlContainer =
                new MySqlContainer(version)
                        .withConfigurationOverride("docker/server-gtids/my.cnf")
                        .withSetupSQL("docker/setup.sql")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(MYSQL_HOST)
                        .withDatabaseName(MYSQL_DATABASE)
                        .withUsername(MYSQL_USER_NAME)
                        .withPassword(MYSQL_USER_PASSWORD)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("mysql-docker-image")));

        return mySqlContainer;
    }

    protected static List<Document> generateTestDataSet(int count) {
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

    protected List<LinkedHashMap<String, Object>> querySql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<LinkedHashMap<String, Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                LinkedHashMap<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = resultSet.getMetaData().getColumnName(i);
                    Object columnValue = resultSet.getObject(i);
                    row.put(columnName, columnValue);
                }
                result.add(row);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    protected void upsertDeleteSourceTable() {
        executeSql(
                "INSERT INTO mysql_cdc.products (name,description,weight)\n"
                        + "VALUES ('car battery','12V car battery',31)");

        executeSql(
                "INSERT INTO mysql_cdc.products (name,description,weight)\n"
                        + "VALUES ('rocks','box of assorted rocks',35)");

        executeSql("DELETE FROM mysql_cdc.products where weight = 35");

        executeSql("UPDATE mysql_cdc.products SET name = 'monster' where weight = 35");
    }

    private void executeSql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            connection.createStatement().execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
