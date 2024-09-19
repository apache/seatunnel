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

import org.apache.seatunnel.shade.com.google.common.io.ByteStreams;
import org.apache.seatunnel.shade.com.google.common.io.CharStreams;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris.IrisCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.connectors.seatunnel.jdbc.utils.JdbcCatalogUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.PullPolicy;
import org.testcontainers.lifecycle.Startables;

import com.github.dockerjava.api.model.Image;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

public abstract class AbstractJdbcIT extends TestSuiteBase implements TestResource {

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
    protected JdbcCase jdbcCase;
    protected Connection connection;
    protected Catalog catalog;
    protected URLClassLoader urlClassLoader;

    abstract JdbcCase getJdbcCase();

    void checkResult(String executeKey, TestContainer container, Container.ExecResult execResult) {}

    abstract String driverUrl();

    abstract Pair<String[], List<SeaTunnelRow>> initTestData();

    abstract GenericContainer<?> initContainer();

    protected URLClassLoader getUrlClassLoader() throws MalformedURLException {
        if (urlClassLoader == null) {
            urlClassLoader =
                    new InsecureURLClassLoader(
                            new URL[] {new URL(driverUrl())},
                            AbstractJdbcIT.class.getClassLoader());
            Thread.currentThread().setContextClassLoader(urlClassLoader);
        }
        return urlClassLoader;
    }

    protected Class<?> loadDriverClassFromUrl() {
        try {
            return getUrlClassLoader().loadClass(jdbcCase.getDriverClass());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + jdbcCase.getDriverClass(), e);
        }
    }

    protected Class<?> loadDriverClass() {
        try {
            return Class.forName(jdbcCase.getDriverClass());
        } catch (Exception e) {
            throw new RuntimeException(
                    "Failed to load driver class: " + jdbcCase.getDriverClass(), e);
        }
    }

    protected void initializeJdbcConnection(String jdbcUrl)
            throws SQLException, InstantiationException, IllegalAccessException {
        Driver driver = (Driver) loadDriverClass().newInstance();
        Properties props = new Properties();

        if (StringUtils.isNotBlank(jdbcCase.getUserName())) {
            props.put("user", jdbcCase.getUserName());
        }

        if (StringUtils.isNotBlank(jdbcCase.getPassword())) {
            props.put("password", jdbcCase.getPassword());
        }

        if (dbServer != null) {
            jdbcUrl = jdbcUrl.replace(HOST, dbServer.getHost());
        }

        this.connection = driver.connect(jdbcUrl, props);
        connection.setAutoCommit(false);
    }

    protected void insertTestData() {
        try (PreparedStatement preparedStatement =
                connection.prepareStatement(jdbcCase.getInsertSql())) {

            List<SeaTunnelRow> rows = jdbcCase.getTestData().getValue();

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

    protected void createSchemaIfNeeded() {}

    protected void createNeededTables() {
        try (Statement statement = connection.createStatement()) {
            String createTemplate = jdbcCase.getCreateSql();

            String createSource =
                    String.format(
                            createTemplate,
                            buildTableInfoWithSchema(
                                    jdbcCase.getDatabase(),
                                    jdbcCase.getSchema(),
                                    jdbcCase.getSourceTable()));
            statement.execute(createSource);

            if (!jdbcCase.isUseSaveModeCreateTable()) {
                if (jdbcCase.getSinkCreateSql() != null) {
                    createTemplate = jdbcCase.getSinkCreateSql();
                }
                String createSink =
                        String.format(
                                createTemplate,
                                buildTableInfoWithSchema(
                                        jdbcCase.getDatabase(),
                                        jdbcCase.getSchema(),
                                        jdbcCase.getSinkTable()));
                statement.execute(createSink);
            }

            connection.commit();
        } catch (Exception exception) {
            log.error(ExceptionUtils.getMessage(exception));
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CREATE_TABLE_FAILED, exception);
        }
    }

    public String insertTable(String schema, String table, String... fields) {
        String columns =
                Arrays.stream(fields).map(this::quoteIdentifier).collect(Collectors.joining(", "));
        String placeholders = Arrays.stream(fields).map(f -> "?").collect(Collectors.joining(", "));

        return "INSERT INTO "
                + buildTableInfoWithSchema(schema, table)
                + " ("
                + columns
                + " )"
                + " VALUES ("
                + placeholders
                + ")";
    }

    protected void clearTable(String database, String schema, String table) {
        clearTable(database, table);
    }

    protected String buildTableInfoWithSchema(String database, String schema, String table) {
        return buildTableInfoWithSchema(database, table);
    }

    public void clearTable(String schema, String table) {
        try (Statement statement = connection.createStatement()) {
            statement.execute("TRUNCATE TABLE " + buildTableInfoWithSchema(schema, table));
            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException exception) {
                throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, exception);
            }
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.CLEAR_TABLE_FAILED, e);
        }
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

    public String buildTableInfoWithSchema(String schema, String table) {
        if (StringUtils.isNotBlank(schema)) {
            return quoteIdentifier(schema) + "." + quoteIdentifier(table);
        } else {
            return quoteIdentifier(table);
        }
    }

    @BeforeAll
    @Override
    public void startUp() {
        dbServer = initContainer().withImagePullPolicy(PullPolicy.alwaysPull());

        Startables.deepStart(Stream.of(dbServer)).join();

        jdbcCase = getJdbcCase();
        beforeStartUP();
        given().ignoreExceptions()
                .await()
                .atMost(360, TimeUnit.SECONDS)
                .untilAsserted(() -> this.initializeJdbcConnection(jdbcCase.getJdbcUrl()));

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
    public void testJdbcDb(TestContainer container)
            throws IOException, InterruptedException, SQLException {
        List<String> configFiles = jdbcCase.getConfigFile();
        for (String configFile : configFiles) {
            try {
                Container.ExecResult execResult = container.executeJob(configFile);
                Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
                checkResult(
                        String.format("%s in [%s]", configFile, container.identifier()),
                        container,
                        execResult);
            } finally {
                clearTable(jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSinkTable());
            }
        }
    }

    protected void initCatalog() {}

    @Test
    public void testCreateIndex() {
        if (catalog == null) {
            return;
        }
        TablePath sourceTablePath =
                new TablePath(
                        jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSourceTable());
        // add suffix for target table
        TablePath targetTablePath =
                new TablePath(
                        jdbcCase.getDatabase(),
                        jdbcCase.getSchema(),
                        jdbcCase.getSinkTable()
                                + ((catalog instanceof OracleCatalog) ? "_INDEX" : "_index"));
        boolean createdDb = false;

        if (!(catalog instanceof IrisCatalog)
                && !catalog.databaseExists(targetTablePath.getDatabaseName())) {
            catalog.createDatabase(targetTablePath, false);
            Assertions.assertTrue(catalog.databaseExists(targetTablePath.getDatabaseName()));
            createdDb = true;
        }

        CatalogTable catalogTable = catalog.getTable(sourceTablePath);

        // not create index
        createIndexOrNot(targetTablePath, catalogTable, false);
        Assertions.assertFalse(hasIndex(catalog, targetTablePath));

        dropTableWithAssert(targetTablePath);
        // create index
        createIndexOrNot(targetTablePath, catalogTable, true);
        Assertions.assertTrue(hasIndex(catalog, targetTablePath));

        dropTableWithAssert(targetTablePath);

        if (createdDb) {
            catalog.dropDatabase(targetTablePath, false);
            Assertions.assertFalse(catalog.databaseExists(targetTablePath.getDatabaseName()));
        }
    }

    private boolean hasIndex(Catalog catalog, TablePath targetTablePath) {
        TableSchema tableSchema = catalog.getTable(targetTablePath).getTableSchema();
        PrimaryKey primaryKey = tableSchema.getPrimaryKey();
        List<ConstraintKey> constraintKeys = tableSchema.getConstraintKeys();
        if (primaryKey != null && StringUtils.isNotBlank(primaryKey.getPrimaryKey())) {
            return true;
        }
        if (!constraintKeys.isEmpty()) {
            return true;
        }
        return false;
    }

    protected void dropTableWithAssert(TablePath targetTablePath) {
        catalog.dropTable(targetTablePath, true);
        Assertions.assertFalse(catalog.tableExists(targetTablePath));
    }

    protected void createIndexOrNot(
            TablePath targetTablePath, CatalogTable catalogTable, boolean createIndex) {
        catalog.createTable(targetTablePath, catalogTable, false, createIndex);
        Assertions.assertTrue(catalog.tableExists(targetTablePath));
    }

    @Test
    public void testCatalog() {
        if (catalog == null) {
            return;
        }
        TablePath sourceTablePath =
                new TablePath(
                        jdbcCase.getDatabase(), jdbcCase.getSchema(), jdbcCase.getSourceTable());
        TablePath targetTablePath =
                new TablePath(
                        jdbcCase.getCatalogDatabase(),
                        jdbcCase.getCatalogSchema(),
                        jdbcCase.getCatalogTable());
        boolean createdDb = false;

        if (!catalog.databaseExists(targetTablePath.getDatabaseName())) {
            catalog.createDatabase(targetTablePath, false);
            Assertions.assertTrue(catalog.databaseExists(targetTablePath.getDatabaseName()));
            createdDb = true;
        }

        CatalogTable catalogTable = catalog.getTable(sourceTablePath);
        catalog.createTable(targetTablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(targetTablePath));

        catalog.dropTable(targetTablePath, false);
        Assertions.assertFalse(catalog.tableExists(targetTablePath));

        if (createdDb) {
            catalog.dropDatabase(targetTablePath, false);
            Assertions.assertFalse(catalog.databaseExists(targetTablePath.getDatabaseName()));
        }

        TableNotExistException exception =
                Assertions.assertThrows(
                        TableNotExistException.class,
                        () ->
                                catalog.truncateTable(
                                        TablePath.of("not_exist", "not_exist", "not_exist"),
                                        false));
        Assertions.assertEquals(
                String.format(
                        "ErrorCode:[API-05], ErrorDescription:[Table not existed] - Table not_exist.not_exist.not_exist does not exist in Catalog %s.",
                        catalog.name()),
                exception.getMessage());
    }

    @Test
    public void testCatalogWithCatalogUtils() throws SQLException, ClassNotFoundException {
        if (StringUtils.isBlank(jdbcCase.getTablePathFullName())) {
            return;
        }

        List<JdbcSourceTableConfig> tablesConfig = new ArrayList<>();
        JdbcSourceTableConfig tableConfig =
                JdbcSourceTableConfig.builder()
                        .query("SELECT * FROM " + jdbcCase.getSourceTable())
                        .useSelectCount(false)
                        .build();
        tablesConfig.add(tableConfig);
        Map<TablePath, JdbcSourceTable> tables =
                JdbcCatalogUtils.getTables(
                        JdbcConnectionConfig.builder()
                                .url(jdbcCase.getJdbcUrl().replace(HOST, dbServer.getHost()))
                                .driverName(jdbcCase.getDriverClass())
                                .username(jdbcCase.getUserName())
                                .password(jdbcCase.getPassword())
                                .build(),
                        tablesConfig);
        Set<TablePath> tablePaths = tables.keySet();

        tablePaths.forEach(
                tablePath -> {
                    log.info(
                            "Expected: {} Actual: {}",
                            tablePath.getFullName(),
                            jdbcCase.getTablePathFullName());
                    Assertions.assertTrue(
                            tablePath
                                    .getFullName()
                                    .equalsIgnoreCase(jdbcCase.getTablePathFullName()));
                });
    }

    protected Object[] toArrayResult(ResultSet resultSet, String[] fieldNames)
            throws SQLException, IOException {
        List<Object> result = new ArrayList<>(0);
        while (resultSet.next()) {
            Object[] rowArray = new Object[fieldNames.length];
            for (int colIndex = 0; colIndex < fieldNames.length; colIndex++) {
                rowArray[colIndex] = checkData(resultSet.getObject(fieldNames[colIndex]));
            }
            result.add(rowArray);
        }
        return result.toArray();
    }

    private Object checkData(Object data) throws SQLException, IOException {
        if (data == null) {
            return null;
        } else if (data instanceof byte[]) {
            return data;
        } else if (data instanceof Clob) {
            try (Reader reader = ((Clob) data).getCharacterStream()) {
                return CharStreams.toString(reader);
            }
        } else if (data instanceof Blob) {
            try (InputStream inputStream = ((Blob) data).getBinaryStream()) {
                return ByteStreams.toByteArray(inputStream);
            }
        } else if (data instanceof InputStream) {
            try (InputStream inputStream = (InputStream) data) {
                return ByteStreams.toByteArray(inputStream);
            }
        } else if (data instanceof Array) {
            Object[] jdbcArray = (Object[]) ((Array) data).getArray();
            Object[] javaArray = new Object[jdbcArray.length];
            for (int index = 0; index < jdbcArray.length; index++) {
                javaArray[index] = checkData(jdbcArray[index]);
            }
            return javaArray;
        } else {
            return data;
        }
    }

    protected void defaultCompare(String executeKey, String[] fieldNames, String sortKey) {
        try (Statement statement = connection.createStatement()) {
            ResultSet source =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM %s ORDER BY %s",
                                    buildTableInfoWithSchema(
                                            this.jdbcCase.getSchema(),
                                            this.jdbcCase.getSourceTable()),
                                    quoteIdentifier(sortKey)));
            Object[] sourceResult = toArrayResult(source, fieldNames);
            ResultSet sink =
                    statement.executeQuery(
                            String.format(
                                    "SELECT * FROM %s ORDER BY %s",
                                    buildTableInfoWithSchema(
                                            this.jdbcCase.getSchema(),
                                            this.jdbcCase.getSinkTable()),
                                    quoteIdentifier(sortKey)));
            Object[] sinkResult = toArrayResult(sink, fieldNames);
            log.warn(
                    "{}: source data count {}, sink data count {}.",
                    executeKey,
                    sourceResult.length,
                    sinkResult.length);
            Assertions.assertArrayEquals(
                    sourceResult, sinkResult, String.format("[%s] data compare", executeKey));
        } catch (SQLException | IOException e) {
            throw new SeaTunnelRuntimeException(JdbcITErrorCode.DATA_COMPARISON_FAILED, e);
        }
    }
}
