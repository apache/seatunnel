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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.github.dockerjava.api.model.Image;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public abstract class AbstractJdbcWildcardIT extends TestSuiteBase implements TestResource {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    protected static final String HOST = "HOST";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/Jdbc/lib && cd /tmp/seatunnel/plugins/Jdbc/lib && wget "
                                        + driverUrl()
                                        + " --no-check-certificate");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    protected GenericContainer<?> dbServer;
    protected JdbcWildcardCase jdbcWildcardCase;
    protected Connection connection;
    protected Catalog catalog;
    protected URLClassLoader urlClassLoader;
    private final String QUERY_SQL = "select * from %s.%s";

    abstract JdbcWildcardCase getJdbcWildcardsCase();

    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {}

    abstract String driverUrl();

    protected Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames = new String[] {"id", "name", "desc"};
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            rows.add(new SeaTunnelRow(new Object[] {i, "name_" + i, "desc_" + i}));
        }
        return Pair.of(fieldNames, rows);
    }

    abstract GenericContainer<?> initContainer();

    protected URLClassLoader getUrlClassLoader() throws MalformedURLException {
        if (urlClassLoader == null) {
            urlClassLoader =
                    new InsecureURLClassLoader(
                            new URL[] {new URL(driverUrl())},
                            AbstractJdbcWildcardIT.class.getClassLoader());
            Thread.currentThread().setContextClassLoader(urlClassLoader);
        }
        return urlClassLoader;
    }

    protected Class<?> loadDriverClassFromUrl() {
        try {
            return getUrlClassLoader().loadClass(jdbcWildcardCase.getDriverClass());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + jdbcWildcardCase.getDriverClass(), e);
        }
    }

    protected Class<?> loadDriverClass() {
        try {
            return Class.forName(jdbcWildcardCase.getDriverClass());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + jdbcWildcardCase.getDriverClass(), e);
        }
    }

    protected void initializeJdbcConnection(String jdbcUrl)
            throws SQLException, InstantiationException, IllegalAccessException {
        Driver driver = (Driver) loadDriverClass().newInstance();
        Properties props = new Properties();

        if (StringUtils.isNotBlank(jdbcWildcardCase.getUserName())) {
            props.put("user", jdbcWildcardCase.getUserName());
        }

        if (StringUtils.isNotBlank(jdbcWildcardCase.getPassword())) {
            props.put("password", jdbcWildcardCase.getPassword());
        }

        if (dbServer != null) {
            jdbcUrl = jdbcUrl.replace(HOST, dbServer.getHost());
        }

        this.connection = driver.connect(jdbcUrl, props);
        connection.setAutoCommit(false);
    }

    protected void insertTestData() {
        for (String tableName : jdbcWildcardCase.getSourceTable()) {
            String insertSQL =
                    String.format(
                            jdbcWildcardCase.getInsertDataTableTemplate(),
                            buildTableInfoWithSchema(
                                    jdbcWildcardCase.getSourceDatabase(), tableName));
            try (PreparedStatement preparedStatement = connection.prepareStatement(insertSQL)) {
                List<SeaTunnelRow> rows = jdbcWildcardCase.getTestData().getValue();
                for (SeaTunnelRow row : rows) {
                    for (int index = 0; index < row.getArity(); index++) {
                        preparedStatement.setObject(index + 1, row.getField(index));
                    }
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
                connection.commit();
            } catch (Exception exception) {
                log.error(ExceptionUtils.getMessage(exception));
                throw new SeaTunnelRuntimeException(JdbcITErrorCode.INSERT_DATA_FAILED, exception);
            }
        }
    }

    protected void createSchemaIfNeeded() {}

    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            String createTemplate = jdbcWildcardCase.getCreateDatabaseTemplate();
            if (StringUtils.isNotBlank(createTemplate)) {
                String createSourcDatabase =
                        String.format(
                                createTemplate,
                                quoteIdentifier(jdbcWildcardCase.getSourceDatabase()));
                statement.execute(createSourcDatabase);

                String createSinkDatabase =
                        String.format(
                                createTemplate,
                                quoteIdentifier(jdbcWildcardCase.getSinkDatabase()));
                statement.execute(createSinkDatabase);
            }
            // create source table
            String createTableTemplate = jdbcWildcardCase.getCreateTableTemplate();
            for (String tableName : jdbcWildcardCase.getSourceTable()) {
                String createSourceTable =
                        String.format(
                                createTableTemplate,
                                buildTableInfoWithSchema(
                                        jdbcWildcardCase.getSourceDatabase(), tableName));
                log.info("create source table ddl: {}", createSourceTable);
                statement.execute(createSourceTable);
            }
            connection.setAutoCommit(false);
            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(database, table);
    }

    /**
     * Some rdbms need quote field.
     *
     * @param field field of rdbms.
     * @return quoted field.
     */
    public String quoteIdentifier(String field) {
        return "`" + field + "`";
    }

    protected String buildDatabaseWithSchema(String database) {
        return quoteIdentifier(database);
    }

    protected String buildTableInfoWithSchema(String schema, String table) {
        if (StringUtils.isNotBlank(schema)) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }

    @BeforeAll
    @Override
    public void startUp() {
        //        dbServer = initContainer().withImagePullPolicy(PullPolicy.alwaysPull());
        dbServer = initContainer();

        Startables.deepStart(Stream.of(dbServer)).join();

        jdbcWildcardCase = getJdbcWildcardsCase();
        beforeStartUP();
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcWildcardCase.getJdbcUrl()));

        createSchemaIfNeeded();
        createNeededTables();
        insertTestData();
        initCatalog();
    }

    // before startUp For example, create a user
    protected void beforeStartUP() {}

    @AfterAll
    @Override
    public void tearDown() throws SQLException {
        if (catalog != null) {
            catalog.close();
        }

        if (connection != null) {
            connection.close();
        }

        if (dbServer != null) {
            dbServer.close();
            String images =
                    dockerClient.listImagesCmd().exec().stream()
                            .map(Image::getId)
                            .collect(Collectors.joining(","));
            log.info(
                    "before remove image {}, list images: {}",
                    dbServer.getDockerImageName(),
                    images);
            try {
                dockerClient.removeImageCmd(dbServer.getDockerImageName()).exec();
            } catch (Exception ignored) {
                log.warn("Failed to delete the image. Another container may be in use", ignored);
            }
            images =
                    dockerClient.listImagesCmd().exec().stream()
                            .map(Image::getId)
                            .collect(Collectors.joining(","));
            log.info(
                    "after remove image {}, list images: {}",
                    dbServer.getDockerImageName(),
                    images);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "")
    public void testJdbcByWildcardsConfig(TestContainer container)
            throws IOException, InterruptedException {
        dropTableIfExists();
        Container.ExecResult execResult = container.executeJob(jdbcWildcardCase.getConfigFile());
        Assertions.assertEquals(0, execResult.getExitCode());
        Awaitility.given()
                .pollDelay(20, TimeUnit.SECONDS)
                .pollInterval(2000, TimeUnit.MILLISECONDS)
                .await()
                .atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertAll(
                                    () -> {
                                        log.info(
                                                query(
                                                                getQuerySQL(
                                                                        jdbcWildcardCase
                                                                                .getSinkDatabase(),
                                                                        jdbcWildcardCase
                                                                                .getSinkTable()
                                                                                .get(0)))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(
                                                        getQuerySQL(
                                                                jdbcWildcardCase
                                                                        .getSourceDatabase(),
                                                                jdbcWildcardCase
                                                                        .getSourceTable()
                                                                        .get(0))),
                                                query(
                                                        getQuerySQL(
                                                                jdbcWildcardCase.getSinkDatabase(),
                                                                jdbcWildcardCase
                                                                        .getSinkTable()
                                                                        .get(0))));
                                    },
                                    () -> {
                                        log.info(
                                                query(
                                                                getQuerySQL(
                                                                        jdbcWildcardCase
                                                                                .getSinkDatabase(),
                                                                        jdbcWildcardCase
                                                                                .getSinkTable()
                                                                                .get(1)))
                                                        .toString());
                                        Assertions.assertIterableEquals(
                                                query(
                                                        getQuerySQL(
                                                                jdbcWildcardCase
                                                                        .getSourceDatabase(),
                                                                jdbcWildcardCase
                                                                        .getSourceTable()
                                                                        .get(1))),
                                                query(
                                                        getQuerySQL(
                                                                jdbcWildcardCase.getSinkDatabase(),
                                                                jdbcWildcardCase
                                                                        .getSinkTable()
                                                                        .get(1))));
                                    });
                        });
    }

    protected void dropTableIfExists() {
        try (Statement statement = connection.createStatement()) {
            connection.setAutoCommit(true);
            for (String table : jdbcWildcardCase.getSinkTable()) {
                if ("oracle".equals(jdbcWildcardCase.getDatabaseType())) {
                    try (ResultSet resultSet =
                            statement.executeQuery(
                                    "SELECT COUNT(*) FROM all_tables WHERE table_name = '"
                                            + table.toUpperCase()
                                            + "'")) {
                        if (resultSet.next() && resultSet.getInt(1) > 0) {
                            statement.execute("DROP TABLE " + table);
                        }
                    }
                } else {
                    statement.execute(
                            "DROP TABLE IF EXISTS "
                                    + buildTableInfoWithSchema(
                                            jdbcWildcardCase.getSinkDatabase(), table));
                }
            }
            connection.setAutoCommit(false);
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    protected void initCatalog() {}

    private List<List<Object>> query(String sql) {
        try (Statement statement = connection.createStatement();
                ResultSet resultSet = statement.executeQuery(sql)) {
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                log.debug(String.format("Print query, sql: %s, data: %s", sql, objects));
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private String getQuerySQL(String databaseName, String tableName) {
        return String.format(
                QUERY_SQL, buildDatabaseWithSchema(databaseName), quoteIdentifier(tableName));
    }
}
