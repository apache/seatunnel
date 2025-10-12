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

package org.apache.seatunnel.e2e.connector.iotdb;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;
import shaded.org.apache.iotdb.isession.ITableSession;
import shaded.org.apache.iotdb.isession.SessionDataSet;
import shaded.org.apache.iotdb.rpc.IoTDBConnectionException;
import shaded.org.apache.iotdb.rpc.StatementExecutionException;
import shaded.org.apache.iotdb.session.TableSessionBuilder;
import shaded.org.apache.tsfile.enums.ColumnCategory;
import shaded.org.apache.tsfile.enums.TSDataType;
import shaded.org.apache.tsfile.read.common.Field;
import shaded.org.apache.tsfile.read.common.RowRecord;
import shaded.org.apache.tsfile.utils.Binary;
import shaded.org.apache.tsfile.write.record.Tablet;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "There is a conflict of thrift version between IoTDB and Spark.Therefore. Refactor starter module, so disabled in spark")
public class IoTDBRelationalIT extends TestSuiteBase implements TestResource {

    private static final String IOTDB_DOCKER_IMAGE = "apache/iotdb:2.0.5-standalone";
    private static final String IOTDB_HOST = "flink_e2e_iotdb_sink";
    private static final int IOTDB_PORT = 6667;
    private static final String IOTDB_USERNAME = "root";
    private static final String IOTDB_PASSWORD = "root";
    private static final String SOURCE_DATABASE = "testSourceDatabase";
    private static final String SINK_DATABASE = "testSinkDatabase";

    private GenericContainer<?> iotdbServer;
    private TableSessionBuilder tableSessionBuilder;
    private ITableSession tableSession;
    private List<RowRecord> testTableDataSet;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        iotdbServer =
                new GenericContainer<>(IOTDB_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(IOTDB_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(IOTDB_DOCKER_IMAGE)));
        iotdbServer.setPortBindings(Lists.newArrayList(String.format("%s:6667", IOTDB_PORT)));
        Startables.deepStart(Stream.of(iotdbServer)).join();
        log.info("IoTDB container started");
        tableSessionBuilder = createTableSessionBuilder();
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .until(
                        () -> {
                            tableSession = tableSessionBuilder.build();
                            return tableSession != null;
                        });

        testTableDataSet = generateTestTableDataSet();
    }

    @TestTemplate
    public void testIoTDBTable(TestContainer container) throws Exception {
        Container.ExecResult execResult =
                container.executeJob("/iotdb/iotdb_source_to_sink_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        List<RowRecord> sinkTableDataset = readSinkTableDataset();
        assertDatasetEquals(testTableDataSet, sinkTableDataset);
    }

    private TableSessionBuilder createTableSessionBuilder() throws IoTDBConnectionException {
        TableSessionBuilder tableSessionBuilder = new TableSessionBuilder();
        List<String> nodeUrls = new ArrayList<>();
        nodeUrls.add("localhost:" + IOTDB_PORT);
        tableSessionBuilder.nodeUrls(nodeUrls);
        tableSessionBuilder.username(IOTDB_USERNAME);
        tableSessionBuilder.password(IOTDB_PASSWORD);
        tableSessionBuilder.database(SOURCE_DATABASE);
        tableSessionBuilder.enableCompression(false);
        return tableSessionBuilder;
    }

    private List<RowRecord> generateTestTableDataSet()
            throws IoTDBConnectionException, StatementExecutionException {
        tableSession.executeNonQueryStatement(
                String.format("CREATE DATABASE IF NOT EXISTS %s", SOURCE_DATABASE));
        List<String> columnNames =
                Arrays.asList(
                        "c_tag",
                        "c_attribute",
                        "c_boolean",
                        "c_tinyint",
                        "c_smallint",
                        "c_int",
                        "c_bigint",
                        "c_float",
                        "c_double",
                        "c_string",
                        "c_text",
                        "c_date",
                        "c_timestamp",
                        "c_blob");
        List<ColumnCategory> columnCategories = new ArrayList<>();
        columnCategories.add(ColumnCategory.TAG);
        columnCategories.add(ColumnCategory.ATTRIBUTE);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        columnCategories.add(ColumnCategory.FIELD);
        List<TSDataType> columnTypes = new ArrayList<>();
        columnTypes.add(TSDataType.STRING);
        columnTypes.add(TSDataType.STRING);
        columnTypes.add(TSDataType.BOOLEAN);
        columnTypes.add(TSDataType.INT32);
        columnTypes.add(TSDataType.INT32);
        columnTypes.add(TSDataType.INT32);
        columnTypes.add(TSDataType.INT64);
        columnTypes.add(TSDataType.FLOAT);
        columnTypes.add(TSDataType.DOUBLE);
        columnTypes.add(TSDataType.STRING);
        columnTypes.add(TSDataType.TEXT);
        columnTypes.add(TSDataType.DATE);
        columnTypes.add(TSDataType.TIMESTAMP);
        columnTypes.add(TSDataType.BLOB);
        Tablet tb = new Tablet("testTable", columnNames, columnTypes, columnCategories);

        List<RowRecord> rowRecords = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            long timestamp = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(i);
            RowRecord record = new RowRecord(timestamp);
            record.addField(new Binary(("tag" + i).getBytes()), TSDataType.STRING);
            record.addField(new Binary(("attr" + i).getBytes()), TSDataType.STRING);
            record.addField(Boolean.FALSE, TSDataType.BOOLEAN);
            record.addField(Byte.valueOf(Byte.MAX_VALUE).intValue(), TSDataType.INT32);
            record.addField(Short.valueOf(Short.MAX_VALUE).intValue(), TSDataType.INT32);
            record.addField(Integer.valueOf(i), TSDataType.INT32);
            record.addField(Long.MAX_VALUE, TSDataType.INT64);
            record.addField(Float.MAX_VALUE, TSDataType.FLOAT);
            record.addField(Double.MAX_VALUE, TSDataType.DOUBLE);
            record.addField(new Binary("testText".getBytes()), TSDataType.TEXT);
            LocalDate ld = LocalDate.of(2024, 12, 25);
            record.addField(20241225, TSDataType.DATE);
            record.addField(timestamp, TSDataType.TIMESTAMP);
            record.addField(new Binary("0x3939".getBytes()), TSDataType.BLOB);
            rowRecords.add(record);
            log.info("TestTableDataSet row: {}", record);
            System.out.printf("TestTableDataSet row: %s%n", record);

            tb.addTimestamp(i, timestamp);
            tb.addValue(i, 0, "tag" + i);
            tb.addValue(i, 1, "attr" + i);
            tb.addValue(i, 2, Boolean.FALSE);
            tb.addValue(i, 3, Byte.MAX_VALUE);
            tb.addValue(i, 4, Short.MAX_VALUE);
            tb.addValue(i, 5, i);
            tb.addValue(i, 6, Long.MAX_VALUE);
            tb.addValue(i, 7, Float.MAX_VALUE);
            tb.addValue(i, 8, Double.MAX_VALUE);
            tb.addValue(i, 9, "testString");
            tb.addValue(i, 10, "testText");
            tb.addValue(i, 11, ld);
            tb.addValue(i, 12, timestamp);
            tb.addValue(i, 13, "99");
        }

        tableSession.insert(tb);
        return rowRecords;
    }

    private List<RowRecord> readSinkTableDataset()
            throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet dataSet =
                tableSession.executeQueryStatement(
                        "SELECT time, c_tag, c_attribute, c_boolean, c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double, c_text, c_date, c_timestamp, c_blob FROM "
                                + SINK_DATABASE
                                + ".testString");
        List<RowRecord> results = new ArrayList<>();
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            results.add(record);
            log.info("TableSinkDataset row: {}", record);
        }
        return results;
    }

    private void assertDatasetEquals(List<RowRecord> testDataset, List<RowRecord> sinkDataset) {
        Assertions.assertEquals(testDataset.size(), sinkDataset.size());

        Collections.sort(testDataset, Comparator.comparingLong(RowRecord::getTimestamp));
        Collections.sort(sinkDataset, Comparator.comparingLong(d -> d.getField(0).getLongV()));
        for (int rowIndex = 0; rowIndex < testDataset.size(); rowIndex++) {
            RowRecord testDatasetRow = testDataset.get(rowIndex);
            RowRecord sinkDatasetRow = sinkDataset.get(rowIndex);
            Assertions.assertEquals(
                    testDatasetRow.getTimestamp(), sinkDatasetRow.getField(0).getLongV());

            List<Field> testDatasetRowFields = testDatasetRow.getFields();
            List<Field> sinkDatasetRowFields = sinkDatasetRow.getFields();
            Assertions.assertEquals(testDatasetRowFields.size(), sinkDatasetRowFields.size() - 1);
            for (int fieldIndex = 0; fieldIndex < testDatasetRowFields.size(); fieldIndex++) {
                Field testDatasetRowField = testDatasetRowFields.get(fieldIndex);
                Field sinkDatasetRowField = sinkDatasetRowFields.get(fieldIndex + 1);
                Assertions.assertEquals(
                        testDatasetRowField.getObjectValue(testDatasetRowField.getDataType()),
                        sinkDatasetRowField.getObjectValue(sinkDatasetRowField.getDataType()));
            }
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (tableSession != null) {
            tableSession.close();
        }
        if (iotdbServer != null) {
            iotdbServer.stop();
        }
    }
}
