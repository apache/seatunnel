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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlContainer;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.MySqlVersion;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.testutils.UniqueDatabase;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;
import org.apache.seatunnel.connectors.seatunnel.paimon.data.PaimonTypeMapper;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.apache.commons.lang3.tuple.ImmutableTriple;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.schema.Column;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.await;

@Slf4j
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason =
                "Currently SPARK do not support cdc. In addition, currently only the zeta engine supports schema evolution for pr https://github.com/apache/seatunnel/pull/5125.")
public class PaimonSinkWithSchemaEvolutionIT extends AbstractPaimonIT implements TestResource {

    private static final String MYSQL_DATABASE = "shop";
    private static final String SOURCE_TABLE = "products";

    private static final String MYSQL_HOST = "mysql_cdc_e2e";
    private static final String MYSQL_USER_NAME = "mysqluser";
    private static final String MYSQL_USER_PASSWORD = "mysqlpw";

    private static final String QUERY = "select * from %s.%s";

    private static final MySqlContainer MYSQL_CONTAINER = createMySqlContainer(MySqlVersion.V8_0);

    private final UniqueDatabase shopDatabase =
            new UniqueDatabase(
                    MYSQL_CONTAINER, MYSQL_DATABASE, "mysqluser", "mysqlpw", MYSQL_DATABASE);

    private static MySqlContainer createMySqlContainer(MySqlVersion version) {
        return new MySqlContainer(version)
                .withConfigurationOverride("docker/server-gtids/my.cnf")
                .withSetupSQL("docker/setup.sql")
                .withNetwork(NETWORK)
                .withNetworkAliases(MYSQL_HOST)
                .withDatabaseName(MYSQL_DATABASE)
                .withUsername(MYSQL_USER_NAME)
                .withPassword(MYSQL_USER_PASSWORD)
                .withLogConsumer(
                        new Slf4jLogConsumer(DockerLoggerFactory.getLogger("mysql-docker-image")));
    }

    private String driverUrl() {
        return "https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.32/mysql-connector-j-8.0.32.jar";
    }

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/MySQL-CDC/lib && cd /tmp/seatunnel/plugins/MySQL-CDC/lib && wget "
                                        + driverUrl());
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        log.info("The second stage: Starting Mysql containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        log.info("Mysql Containers are started");
        shopDatabase.createAndInitialize();
        log.info("Mysql ddl execution is complete");
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (MYSQL_CONTAINER != null) {
            MYSQL_CONTAINER.close();
        }
    }

    @TestTemplate
    public void testMysqlCdcSinkPaimonWithSchemaChange(TestContainer container) throws Exception {
        String jobConfigFile = "/mysql_cdc_to_paimon_with_schema_change.conf";
        CompletableFuture.runAsync(
                () -> {
                    try {
                        container.executeJob(jobConfigFile);
                    } catch (Exception e) {
                        log.error("Commit task exception :" + e.getMessage());
                        throw new RuntimeException(e);
                    }
                });

        // Waiting for auto create sink table
        Thread.sleep(15000);

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            Assertions.assertIterableEquals(
                                    queryMysql(String.format(QUERY, MYSQL_DATABASE, SOURCE_TABLE)),
                                    queryPaimon(null, 0, Integer.MAX_VALUE));
                        });

        // Case 1: Add columns with data at same time
        shopDatabase.setTemplateName("add_columns").createAndInitialize();
        // Because the paimon is not supported default value, so when the source table add columns
        // with default value at same time, the history data in paimon has no value.
        List<ImmutableTriple<String[], Integer, Integer>> idRangesWithFiledProjection1 =
                getIdRangesWithFiledProjectionImmutableTriplesCase1();
        vertifySchemaAndData(container, idRangesWithFiledProjection1);

        // Case 2: Drop columns with data at same time
        shopDatabase.setTemplateName("drop_columns").createAndInitialize();
        List<ImmutableTriple<String[], Integer, Integer>> idRangesWithFiledProjection2 =
                getIdRangesWithFiledProjectionImmutableTriplesCase2();
        vertifySchemaAndData(container, idRangesWithFiledProjection2);

        // Case 3: Change columns with data at same time
        shopDatabase.setTemplateName("change_columns").createAndInitialize();
        List<ImmutableTriple<String[], Integer, Integer>> idRangesWithFiledProjection3 =
                getIdRangesWithFiledProjectionImmutableTriplesCase3();
        vertifySchemaAndData(container, idRangesWithFiledProjection3);

        // Case 4: Modify columns with data at same time
        shopDatabase.setTemplateName("modify_columns").createAndInitialize();
        List<ImmutableTriple<String[], Integer, Integer>> idRangesWithFiledProjection4 =
                getIdRangesWithFiledProjectionImmutableTriplesCase4();
        vertifySchemaAndData(container, idRangesWithFiledProjection4);
    }

    private List<ImmutableTriple<String[], Integer, Integer>>
            getIdRangesWithFiledProjectionImmutableTriplesCase4() {
        List<ImmutableTriple<String[], Integer, Integer>> newIdRangesWithFiledProjection =
                getIdRangesWithFiledProjectionImmutableTriplesCase3();
        newIdRangesWithFiledProjection.add(
                ImmutableTriple.of(
                        new String[] {"id", "name", "description", "weight", "add_column"},
                        164,
                        172));
        return newIdRangesWithFiledProjection;
    }

    private List<ImmutableTriple<String[], Integer, Integer>>
            getIdRangesWithFiledProjectionImmutableTriplesCase3() {
        String changeColumnNameBefore = "add_column2";
        String changeColumnNameAfter = "add_column";
        List<ImmutableTriple<String[], Integer, Integer>>
                idRangesWithFiledProjectionImmutableTriplesCase2 =
                        getIdRangesWithFiledProjectionImmutableTriplesCase2();
        List<ImmutableTriple<String[], Integer, Integer>> newIdRangesWithFiledProjection =
                idRangesWithFiledProjectionImmutableTriplesCase2.stream()
                        .map(
                                immutableTriple ->
                                        ImmutableTriple.of(
                                                Arrays.stream(immutableTriple.getLeft())
                                                        .map(
                                                                column ->
                                                                        column.equals(
                                                                                        changeColumnNameBefore)
                                                                                ? changeColumnNameAfter
                                                                                : column)
                                                        .toArray(String[]::new),
                                                immutableTriple.getMiddle(),
                                                immutableTriple.getRight()))
                        .collect(Collectors.toList());
        newIdRangesWithFiledProjection.add(
                ImmutableTriple.of(
                        new String[] {"id", "name", "description", "weight", "add_column"},
                        155,
                        163));
        return newIdRangesWithFiledProjection;
    }

    private List<ImmutableTriple<String[], Integer, Integer>>
            getIdRangesWithFiledProjectionImmutableTriplesCase2() {
        List<String> dropColumnNames =
                Arrays.asList("add_column4", "add_column6", "add_column1", "add_column3");
        List<ImmutableTriple<String[], Integer, Integer>>
                idRangesWithFiledProjectionImmutableTriplesCase1 =
                        getIdRangesWithFiledProjectionImmutableTriplesCase1();
        List<ImmutableTriple<String[], Integer, Integer>> newIdRangesWithFiledProjection =
                idRangesWithFiledProjectionImmutableTriplesCase1.stream()
                        .map(
                                immutableTriple ->
                                        ImmutableTriple.of(
                                                Arrays.stream(immutableTriple.getLeft())
                                                        .filter(
                                                                column ->
                                                                        !dropColumnNames.contains(
                                                                                column))
                                                        .toArray(String[]::new),
                                                immutableTriple.getMiddle(),
                                                immutableTriple.getRight()))
                        .collect(Collectors.toList());

        newIdRangesWithFiledProjection.add(
                ImmutableTriple.of(
                        new String[] {"id", "name", "description", "weight", "add_column2"},
                        137,
                        154));
        return newIdRangesWithFiledProjection;
    }

    private static List<ImmutableTriple<String[], Integer, Integer>>
            getIdRangesWithFiledProjectionImmutableTriplesCase1() {
        return new ArrayList<ImmutableTriple<String[], Integer, Integer>>() {
            {
                add(
                        ImmutableTriple.of(
                                new String[] {"id", "name", "description", "weight"}, 0, 118));
                add(
                        ImmutableTriple.of(
                                new String[] {
                                    "id",
                                    "name",
                                    "description",
                                    "weight",
                                    "add_column1",
                                    "add_column2"
                                },
                                119,
                                127));
                add(
                        ImmutableTriple.of(
                                new String[] {
                                    "id",
                                    "name",
                                    "description",
                                    "weight",
                                    "add_column1",
                                    "add_column2",
                                    "add_column3",
                                    "add_column4"
                                },
                                128,
                                136));
                add(
                        ImmutableTriple.of(
                                new String[] {
                                    "id",
                                    "add_column6",
                                    "name",
                                    "description",
                                    "weight",
                                    "add_column1",
                                    "add_column2",
                                    "add_column3",
                                    "add_column4"
                                },
                                173,
                                181));
            }
        };
    }

    private void vertifySchemaAndData(
            TestContainer container,
            List<ImmutableTriple<String[], Integer, Integer>> idRangesWithFiledProjection) {
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // copy paimon to local
                            container.executeExtraCommands(containerExtendedFactory);
                            // 1. Vertify the schema
                            vertifySchema();

                            // 2. Vertify the data
                            idRangesWithFiledProjection.forEach(
                                    idRange ->
                                            Assertions.assertIterableEquals(
                                                    queryMysql(
                                                            String.format(
                                                                    "select "
                                                                            + String.join(
                                                                                    ",",
                                                                                    Arrays.asList(
                                                                                            idRange
                                                                                                    .getLeft()))
                                                                            + " from %s.%s where id >= %s and id <= %s",
                                                                    MYSQL_DATABASE,
                                                                    SOURCE_TABLE,
                                                                    idRange.getMiddle(),
                                                                    idRange.getRight())),
                                                    queryPaimon(
                                                            idRange.getLeft(),
                                                            idRange.getMiddle(),
                                                            idRange.getRight())));
                        });
    }

    private void vertifySchema() {
        try (MySqlCatalog mySqlCatalog =
                new MySqlCatalog(
                        "mysql",
                        MYSQL_USER_NAME,
                        MYSQL_USER_PASSWORD,
                        JdbcUrlUtil.getUrlInfo(MYSQL_CONTAINER.getJdbcUrl()),
                        null)) {
            mySqlCatalog.open();
            CatalogTable mySqlCatalogTable =
                    mySqlCatalog.getTable(TablePath.of(MYSQL_DATABASE, SOURCE_TABLE));
            TableSchema tableSchemaInMysql = mySqlCatalogTable.getTableSchema();

            List<org.apache.seatunnel.api.table.catalog.Column> columns =
                    tableSchemaInMysql.getColumns();
            FileStoreTable table = (FileStoreTable) getTable("mysql_to_paimon", "products");
            List<DataField> fields = table.schema().fields();

            Assertions.assertEquals(fields.size(), columns.size());
            for (int i = 0; i < columns.size(); i++) {
                BasicTypeDefine<DataType> paimonTypeDefine =
                        PaimonTypeMapper.INSTANCE.reconvert(columns.get(i));
                DataField dataField = fields.get(i);
                Assertions.assertEquals(paimonTypeDefine.getName(), dataField.name());
                Assertions.assertEquals(
                        dataField.type().getTypeRoot(),
                        paimonTypeDefine.getNativeType().getTypeRoot());
            }
        }
    }

    private int getColumnIndex(PredicateBuilder builder, Column column) {
        int index = builder.indexOf(column.getColumnName());
        if (index == -1) {
            throw new IllegalArgumentException(
                    String.format("The column named [%s] is not exists", column.getColumnName()));
        }
        return index;
    }

    @SneakyThrows
    protected List<List<Object>> queryPaimon(
            String[] projectionFiles, int lowerBound, int upperBound) {
        FileStoreTable table = (FileStoreTable) getTable("mysql_to_paimon", "products");
        Predicate finalPredicate = getPredicateWithBound(lowerBound, upperBound, table);
        ReadBuilder readBuilder = table.newReadBuilder().withFilter(finalPredicate);
        List<DataField> fields = table.schema().fields();
        if (projectionFiles != null && projectionFiles.length > 0) {
            readBuilder.withProjection(
                    getProjectionIndex(table.schema().fieldNames(), projectionFiles));
            fields =
                    table.schema().fields().stream()
                            .filter(
                                    dataField ->
                                            Arrays.asList(projectionFiles)
                                                    .contains(dataField.name()))
                            .collect(Collectors.toList());
        }
        TableScan.Plan plan = readBuilder.newScan().plan();
        TableRead tableRead = readBuilder.newRead();

        List<List<Object>> results = new ArrayList<>();
        try (RecordReader<InternalRow> reader = tableRead.executeFilter().createReader(plan)) {
            List<DataField> finalFields = fields;
            reader.forEachRemaining(
                    row -> {
                        List<Object> rowRecords = new ArrayList<>();
                        for (int i = 0; i < finalFields.size(); i++) {
                            Object fieldOrNull =
                                    InternalRow.createFieldGetter(finalFields.get(i).type(), i)
                                            .getFieldOrNull(row);
                            if (fieldOrNull instanceof BinaryString) {
                                fieldOrNull = ((BinaryString) fieldOrNull).toString();
                            } else if (fieldOrNull instanceof Timestamp) {
                                fieldOrNull = ((Timestamp) fieldOrNull).toSQLTimestamp();
                            } else if (fieldOrNull instanceof Decimal) {
                                fieldOrNull = ((Decimal) fieldOrNull).toBigDecimal();
                            }
                            rowRecords.add(fieldOrNull);
                        }
                        results.add(rowRecords);
                    });
        }
        return results;
    }

    private Predicate getPredicateWithBound(int lowerBound, int upperBound, FileStoreTable table) {
        PredicateBuilder lowerBoundPredicateBuilder = new PredicateBuilder(table.rowType());
        Predicate lowerBoundPredicate =
                lowerBoundPredicateBuilder.greaterOrEqual(
                        getColumnIndex(lowerBoundPredicateBuilder, new Column("id")), lowerBound);

        PredicateBuilder upperBoundPredicateBuilder = new PredicateBuilder(table.rowType());
        Predicate upperBoundPredicate =
                upperBoundPredicateBuilder.lessOrEqual(
                        getColumnIndex(upperBoundPredicateBuilder, new Column("id")), upperBound);

        return PredicateBuilder.and(lowerBoundPredicate, upperBoundPredicate);
    }

    private int[] getProjectionIndex(List<String> actualFieldNames, String[] projectionFieldNames) {
        return Arrays.stream(projectionFieldNames)
                .mapToInt(
                        projectionFieldName -> {
                            int index = actualFieldNames.indexOf(projectionFieldName);
                            if (index == -1) {
                                throw new IllegalArgumentException(
                                        "column " + projectionFieldName + " does not exist.");
                            }
                            return index;
                        })
                .toArray();
    }

    private Connection getJdbcConnection() throws SQLException {
        return DriverManager.getConnection(
                MYSQL_CONTAINER.getJdbcUrl(),
                MYSQL_CONTAINER.getUsername(),
                MYSQL_CONTAINER.getPassword());
    }

    private List<List<Object>> queryMysql(String sql) {
        try (Connection connection = getJdbcConnection()) {
            ResultSet resultSet = connection.createStatement().executeQuery(sql);
            List<List<Object>> result = new ArrayList<>();
            int columnCount = resultSet.getMetaData().getColumnCount();
            while (resultSet.next()) {
                ArrayList<Object> objects = new ArrayList<>();
                for (int i = 1; i <= columnCount; i++) {
                    objects.add(resultSet.getObject(i));
                }
                result.add(objects);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
