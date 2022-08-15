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

package org.apache.seatunnel.e2e.spark.v2.iotdb;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.e2e.spark.SparkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class FakeSourceToIoTDBIT extends SparkContainer {

    private static final String IOTDB_DOCKER_IMAGE = "apache/iotdb:0.13.1-node";
    private static final String IOTDB_HOST = "spark_e2e_iotdb";
    private static final int IOTDB_PORT = 6668;
    private static final String IOTDB_USERNAME = "root";
    private static final String IOTDB_PASSWORD = "root";

    private GenericContainer<?> iotdbServer;
    private Session session;

    @BeforeEach
    public void startIoTDBContainer() throws Exception {
        iotdbServer = new GenericContainer<>(IOTDB_DOCKER_IMAGE)
                .withNetwork(NETWORK)
                .withNetworkAliases(IOTDB_HOST)
                .withLogConsumer(new Slf4jLogConsumer(log));
        iotdbServer.setPortBindings(Lists.newArrayList(
                String.format("%s:6667", IOTDB_PORT)));
        Startables.deepStart(Stream.of(iotdbServer)).join();
        log.info("IoTDB container started");
        // wait for IoTDB fully start
        session = createSession();
        given().ignoreExceptions()
                .await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> session.open());
        initIoTDBTimeseries();
    }

    /**
     * fake source -> IoTDB sink
     */
    @Test
    public void testFakeSourceToIoTDB() throws Exception {
        Container.ExecResult execResult = executeSeaTunnelSparkJob("/iotdb/fakesource_to_iotdb.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // query result
        SessionDataSet dataSet = session.executeQueryStatement("select status, value from root.ln.d1");
        List<Object> actual = new ArrayList<>();
        while (dataSet.hasNext()) {
            RowRecord row = dataSet.next();
            List<Field> fields = row.getFields();
            Field status = fields.get(0);
            Field val = fields.get(1);
            actual.add(Arrays.asList(status.getBoolV(), val.getLongV()));
        }
        List<Object> expected = Arrays.asList(
                Arrays.asList(Boolean.TRUE, Long.valueOf(1001)),
                Arrays.asList(Boolean.FALSE, Long.valueOf(1002)));
        Assertions.assertIterableEquals(expected, actual);
    }

    private Session createSession() {
        return new Session.Builder()
                .host("localhost")
                .port(IOTDB_PORT)
                .username(IOTDB_USERNAME)
                .password(IOTDB_PASSWORD)
                .build();
    }

    private void initIoTDBTimeseries() throws Exception {
        session.setStorageGroup("root.ln");
        session.createTimeseries("root.ln.d1.status",
                TSDataType.BOOLEAN, TSEncoding.PLAIN, CompressionType.SNAPPY);
        session.createTimeseries("root.ln.d1.value",
                TSDataType.INT64, TSEncoding.PLAIN, CompressionType.SNAPPY);
    }

    @AfterEach
    public void closeIoTDBContainer() {
        if (iotdbServer != null) {
            iotdbServer.stop();
        }
    }
}
