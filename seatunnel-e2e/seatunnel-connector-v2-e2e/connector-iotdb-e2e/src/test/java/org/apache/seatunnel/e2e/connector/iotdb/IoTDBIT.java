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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason =
                "There is a conflict of thrift version between IoTDB and Spark.Therefore. Refactor starter module, so disabled in flink")
public class IoTDBIT extends TestSuiteBase implements TestResource {

    private static final String IOTDB_DOCKER_IMAGE = "apache/iotdb:0.13.1-node";
    private static final String IOTDB_HOST = "flink_e2e_iotdb_sink";
    private static final int IOTDB_PORT = 6667;
    private static final String IOTDB_USERNAME = "root";
    private static final String IOTDB_PASSWORD = "root";
    private static final String SOURCE_GROUP = "root.source_group";
    private static final String SINK_GROUP = "root.sink_group";

    private GenericContainer<?> iotdbServer;
    private Session session;
    private List<RowRecord> testDataset;

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
        // wait for IoTDB fully start
        session = createSession();
        given().ignoreExceptions()
                .await()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> session.open());
        testDataset = generateTestDataSet();
    }

    @TestTemplate
    public void testIoTDB(TestContainer container) throws Exception {
        Container.ExecResult execResult = container.executeJob("/iotdb/iotdb_source_to_sink.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        List<RowRecord> sinkDataset = readSinkDataset();
        assertDatasetEquals(testDataset, sinkDataset);
    }

    private Session createSession() {
        return new Session.Builder()
                .host("localhost")
                .port(IOTDB_PORT)
                .username(IOTDB_USERNAME)
                .password(IOTDB_PASSWORD)
                .build();
    }

    private List<RowRecord> generateTestDataSet()
            throws IoTDBConnectionException, StatementExecutionException {
        session.setStorageGroup(SOURCE_GROUP);
        session.setStorageGroup(SINK_GROUP);

        String[] deviceIds = new String[] {"device_a", "device_b"};
        LinkedHashMap<String, TSDataType> measurements = new LinkedHashMap<>();
        measurements.put("c_string", TSDataType.TEXT);
        measurements.put("c_boolean", TSDataType.BOOLEAN);
        measurements.put("c_tinyint", TSDataType.INT32);
        measurements.put("c_smallint", TSDataType.INT32);
        measurements.put("c_int", TSDataType.INT32);
        measurements.put("c_bigint", TSDataType.INT64);
        measurements.put("c_float", TSDataType.FLOAT);
        measurements.put("c_double", TSDataType.DOUBLE);

        List<RowRecord> rowRecords = new ArrayList<>();
        for (String deviceId : deviceIds) {
            String devicePath = String.format("%s.%s", SOURCE_GROUP, deviceId);
            ArrayList<String> measurementKeys = new ArrayList<>(measurements.keySet());
            for (String measurement : measurements.keySet()) {
                session.createTimeseries(
                        String.format("%s.%s", devicePath, measurement),
                        measurements.get(measurement),
                        TSEncoding.PLAIN,
                        CompressionType.SNAPPY);
                session.createTimeseries(
                        String.format("%s.%s.%s", SINK_GROUP, deviceId, measurement),
                        measurements.get(measurement),
                        TSEncoding.PLAIN,
                        CompressionType.SNAPPY);
            }

            for (int rowCount = 0; rowCount < 100; rowCount++) {
                long timestamp = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(rowCount);
                RowRecord record = new RowRecord(timestamp);
                record.addField(new Binary(deviceId), TSDataType.TEXT);
                record.addField(Boolean.FALSE, TSDataType.BOOLEAN);
                record.addField(Byte.valueOf(Byte.MAX_VALUE).intValue(), TSDataType.INT32);
                record.addField(Short.valueOf(Short.MAX_VALUE).intValue(), TSDataType.INT32);
                record.addField(Integer.valueOf(rowCount), TSDataType.INT32);
                record.addField(Long.MAX_VALUE, TSDataType.INT64);
                record.addField(Float.MAX_VALUE, TSDataType.FLOAT);
                record.addField(Double.MAX_VALUE, TSDataType.DOUBLE);
                rowRecords.add(record);
                log.info("TestDataSet row: {}", record);

                session.insertRecord(
                        devicePath,
                        record.getTimestamp(),
                        measurementKeys,
                        record.getFields().stream()
                                .map(f -> f.getDataType())
                                .collect(Collectors.toList()),
                        record.getFields().stream()
                                .map(f -> f.getObjectValue(f.getDataType()))
                                .collect(Collectors.toList()));
            }
        }
        return rowRecords;
    }

    private List<RowRecord> readSinkDataset()
            throws IoTDBConnectionException, StatementExecutionException {
        SessionDataSet dataSet =
                session.executeQueryStatement(
                        "SELECT c_string, c_boolean, c_tinyint, c_smallint, c_int, c_bigint, c_float, c_double FROM "
                                + SINK_GROUP
                                + ".* align by device");
        List<RowRecord> results = new ArrayList<>();
        while (dataSet.hasNext()) {
            RowRecord record = dataSet.next();
            List<Field> notContainDeviceField =
                    record.getFields().stream()
                            .filter(field -> !field.getStringValue().startsWith(SINK_GROUP))
                            .collect(Collectors.toList());
            record = new RowRecord(record.getTimestamp(), notContainDeviceField);
            results.add(record);
            log.info("SinkDataset row: {}", record);
        }
        return results;
    }

    private void assertDatasetEquals(List<RowRecord> testDataset, List<RowRecord> sinkDataset) {
        Assertions.assertEquals(testDataset.size(), sinkDataset.size());

        Collections.sort(testDataset, Comparator.comparingLong(RowRecord::getTimestamp));
        Collections.sort(sinkDataset, Comparator.comparingLong(RowRecord::getTimestamp));
        for (int rowIndex = 0; rowIndex < testDataset.size(); rowIndex++) {
            RowRecord testDatasetRow = testDataset.get(rowIndex);
            RowRecord sinkDatasetRow = sinkDataset.get(rowIndex);
            Assertions.assertEquals(testDatasetRow.getTimestamp(), sinkDatasetRow.getTimestamp());

            List<Field> testDatasetRowFields = testDatasetRow.getFields();
            List<Field> sinkDatasetRowFields = sinkDatasetRow.getFields();
            Assertions.assertEquals(testDatasetRowFields.size(), sinkDatasetRowFields.size());
            for (int fieldIndex = 0; fieldIndex < testDatasetRowFields.size(); fieldIndex++) {
                Field testDatasetRowField = testDatasetRowFields.get(fieldIndex);
                Field sinkDatasetRowField = sinkDatasetRowFields.get(fieldIndex);
                Assertions.assertEquals(
                        testDatasetRowField.getObjectValue(testDatasetRowField.getDataType()),
                        sinkDatasetRowField.getObjectValue(sinkDatasetRowField.getDataType()));
            }
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (session != null) {
            session.close();
        }
        if (iotdbServer != null) {
            iotdbServer.stop();
        }
    }
}
