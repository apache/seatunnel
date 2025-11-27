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

package org.apache.seatunnel.e2e.connector.fluss;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.alibaba.fluss.client.Connection;
import com.alibaba.fluss.client.ConnectionFactory;
import com.alibaba.fluss.client.admin.Admin;
import com.alibaba.fluss.client.table.Table;
import com.alibaba.fluss.client.table.scanner.ScanRecord;
import com.alibaba.fluss.client.table.scanner.log.LogScanner;
import com.alibaba.fluss.client.table.scanner.log.ScanRecords;
import com.alibaba.fluss.config.Configuration;
import com.alibaba.fluss.metadata.DatabaseDescriptor;
import com.alibaba.fluss.metadata.Schema;
import com.alibaba.fluss.metadata.TableBucket;
import com.alibaba.fluss.metadata.TableDescriptor;
import com.alibaba.fluss.metadata.TablePath;
import com.alibaba.fluss.row.GenericRow;
import com.alibaba.fluss.row.InternalRow;
import com.alibaba.fluss.types.DataTypes;
import com.alibaba.fluss.utils.CloseableIterator;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Socket;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.awaitility.Awaitility.given;

@Slf4j
public class FlussSinkIT extends TestSuiteBase implements TestResource {
    private static final String DOCKER_IMAGE = "fluss/fluss:0.7.0";
    private static final String DOCKER_ZK_IMAGE = "zookeeper:3.9.2";

    private static final String FLUSS_Coordinator_HOST = "fluss_coordinator_e2e";
    private static final String FLUSS_Tablet_HOST = "fluss_tablet_e2e";
    private static final String ZK_HOST = "zk_e2e";
    private static final int ZK_PORT = 2181;
    private static final int FLUSS_Coordinator_PORT = 9123;
    private static final int FLUSS_Tablet_PORT = 9124;
    private static final int FLUSS_Coordinator_LOCAL_PORT = 8123;
    private static final int FLUSS_Tablet_LOCAL_PORT = 8124;

    private GenericContainer<?> zookeeperServer;
    private GenericContainer<?> coordinatorServer;
    private GenericContainer<?> tabletServer;

    private Connection flussConnection;

    private static final String DB_NAME = "fluss_db_test";
    private static final String DB_NAME_2 = "fluss_db_test2";
    private static final String DB_NAME_3 = "fluss_db_test3";
    private static final String TABLE_NAME = "fluss_tb_table1";
    private static final String TABLE_NAME_2 = "fluss_tb_table2";
    private static final String TABLE_NAME_3 = "fluss_tb_table3";

    @BeforeAll
    @Override
    public void startUp() {
        createZookeeperContainer();
        createFlussContainer();
    }

    private void createFlussContainer() {
        log.info("Starting FlussServer container...");
        String coordinatorEnv =
                String.format(
                        "zookeeper.address: %s:%d\n"
                                + "bind.listeners: INTERNAL://%s:%d, LOCALCLIENT://%s:%d \n"
                                + "advertised.listeners: INTERNAL://%s:%d, LOCALCLIENT://localhost:%d\n"
                                + "internal.listener.name: INTERNAL",
                        ZK_HOST,
                        ZK_PORT,
                        FLUSS_Coordinator_HOST,
                        FLUSS_Coordinator_PORT,
                        FLUSS_Coordinator_HOST,
                        FLUSS_Coordinator_LOCAL_PORT,
                        FLUSS_Coordinator_HOST,
                        FLUSS_Coordinator_PORT,
                        FLUSS_Coordinator_LOCAL_PORT);
        coordinatorServer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(FLUSS_Coordinator_HOST)
                        .withEnv("FLUSS_PROPERTIES", coordinatorEnv)
                        .withCommand("coordinatorServer")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("coordinatorServer")));
        coordinatorServer.setPortBindings(
                Lists.newArrayList(
                        String.format(
                                "%s:%s",
                                FLUSS_Coordinator_LOCAL_PORT, FLUSS_Coordinator_LOCAL_PORT)));
        Startables.deepStart(Stream.of(coordinatorServer)).join();
        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(
                        () ->
                                checkPort(
                                        coordinatorServer.getHost(),
                                        FLUSS_Coordinator_LOCAL_PORT,
                                        1000));
        log.info("coordinatorServer container start success");

        String tabletEnv =
                String.format(
                        "zookeeper.address: %s:%d\n"
                                + "bind.listeners: INTERNAL://%s:%d, LOCALCLIENT://%s:%d\n"
                                + "advertised.listeners: INTERNAL://%s:%d, LOCALCLIENT://localhost:%d\n"
                                + "internal.listener.name: INTERNAL\n"
                                + "tablet-server.id: 0\n"
                                + "kv.snapshot.interval: 0s\n"
                                + "data.dir: /tmp/fluss/data\n"
                                + "remote.data.dir: /tmp/fluss/remote-data",
                        ZK_HOST,
                        ZK_PORT,
                        FLUSS_Tablet_HOST,
                        FLUSS_Tablet_PORT,
                        FLUSS_Tablet_HOST,
                        FLUSS_Tablet_LOCAL_PORT,
                        FLUSS_Tablet_HOST,
                        FLUSS_Tablet_PORT,
                        FLUSS_Tablet_LOCAL_PORT);
        tabletServer =
                new GenericContainer<>(DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(FLUSS_Tablet_HOST)
                        .withEnv("FLUSS_PROPERTIES", tabletEnv)
                        .withCommand("tabletServer")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger("tabletServer")));
        tabletServer.setPortBindings(
                Lists.newArrayList(
                        String.format("%s:%s", FLUSS_Tablet_LOCAL_PORT, FLUSS_Tablet_LOCAL_PORT)));
        Startables.deepStart(Stream.of(tabletServer)).join();
        given().ignoreExceptions()
                .await()
                .atMost(120, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .untilAsserted(this::initializeConnection);
        log.info("tabletServer container start success");
        log.info("FlussServer Containers are started");
    }

    private void createZookeeperContainer() {
        log.info("Starting ZookeeperServer container...");
        zookeeperServer =
                new GenericContainer<>(DOCKER_ZK_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(ZK_HOST)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(DOCKER_ZK_IMAGE)));
        zookeeperServer.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", ZK_PORT, ZK_PORT)));
        Startables.deepStart(Stream.of(zookeeperServer)).join();
        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .pollInterval(5, TimeUnit.SECONDS)
                .until(() -> checkPort(zookeeperServer.getHost(), ZK_PORT, 1000));
        log.info("ZookeeperServer Containers are started");
    }

    private void initializeConnection() throws ExecutionException, InterruptedException {
        Configuration flussConfig = new Configuration();
        flussConfig.setString(
                "bootstrap.servers",
                coordinatorServer.getHost() + ":" + FLUSS_Coordinator_LOCAL_PORT);
        flussConnection = ConnectionFactory.createConnection(flussConfig);
        createDb(flussConnection, DB_NAME);
    }

    public void createDb(Connection connection, String dbName)
            throws ExecutionException, InterruptedException {
        Admin admin = connection.getAdmin();
        DatabaseDescriptor descriptor = DatabaseDescriptor.builder().build();
        admin.dropDatabase(dbName, true, true).get();
        admin.createDatabase(dbName, descriptor, true).get();
    }

    public Schema getFlussSchema() {
        return Schema.newBuilder()
                .column("fbytes", DataTypes.BYTES())
                .column("fboolean", DataTypes.BOOLEAN())
                .column("fint", DataTypes.INT())
                .column("ftinyint", DataTypes.TINYINT())
                .column("fsmallint", DataTypes.SMALLINT())
                .column("fbigint", DataTypes.BIGINT())
                .column("ffloat", DataTypes.FLOAT())
                .column("fdouble", DataTypes.DOUBLE())
                .column("fdecimal", DataTypes.DECIMAL(30, 8))
                .column("fstring", DataTypes.STRING())
                .column("fdate", DataTypes.DATE())
                .column("ftime", DataTypes.TIME())
                .column("ftimestamp", DataTypes.TIMESTAMP())
                .column("ftimestamp_ltz", DataTypes.TIMESTAMP_LTZ())
                .primaryKey("fstring")
                .build();
    }

    public void createTable(Connection connection, String dbName, String tableName, Schema schema)
            throws ExecutionException, InterruptedException {
        Admin admin = connection.getAdmin();
        TableDescriptor tableDescriptor = TableDescriptor.builder().schema(schema).build();
        TablePath tablePath = TablePath.of(dbName, tableName);
        admin.dropTable(tablePath, true).get();
        admin.createTable(tablePath, tableDescriptor, true).get(); // blocking call
    }

    public static boolean checkPort(String host, int port, int timeoutMs) throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new java.net.InetSocketAddress(host, port), timeoutMs);
            return true;
        } catch (Exception e) {
            throw e;
        }
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (tabletServer != null) {
            tabletServer.close();
        }
        if (coordinatorServer != null) {
            coordinatorServer.close();
        }
        if (zookeeperServer != null) {
            zookeeperServer.close();
        }
    }

    @TestTemplate
    public void testFlussSink(TestContainer container) throws Exception {
        log.info(" create fluss table");
        createDb(flussConnection, DB_NAME);
        createTable(flussConnection, DB_NAME, TABLE_NAME, getFlussSchema());
        Container.ExecResult execFake2fluss = container.executeJob("/fake_to_fluss.conf");
        Assertions.assertEquals(0, execFake2fluss.getExitCode(), execFake2fluss.getStderr());
        checkFlussData(DB_NAME, TABLE_NAME);
    }

    @TestTemplate
    public void testFlussMultiTableSink(TestContainer container) throws Exception {
        log.info(" create fluss tables");
        createDb(flussConnection, DB_NAME_2);
        createDb(flussConnection, DB_NAME_3);
        createTable(flussConnection, DB_NAME_2, TABLE_NAME, getFlussSchema());
        createTable(flussConnection, DB_NAME_2, TABLE_NAME_2, getFlussSchema());
        createTable(flussConnection, DB_NAME_3, TABLE_NAME_3, getFlussSchema());

        Container.ExecResult execFake2fluss =
                container.executeJob("/fake_to_multipletable_fluss.conf");
        Assertions.assertEquals(0, execFake2fluss.getExitCode(), execFake2fluss.getStderr());
        checkFlussData(DB_NAME_2, TABLE_NAME);
        checkFlussData(DB_NAME_2, TABLE_NAME_2);
        checkFlussData(DB_NAME_3, TABLE_NAME_3);
    }

    public void checkFlussData(String dbName, String tableName) throws IOException {
        // check log data
        List<GenericRow> streamData =
                getFlussTableStreamData(flussConnection, dbName, tableName, 10);
        checkFlussTableStreamData(streamData);
        // check data
        List<GenericRow> data = getFlussTableData(flussConnection, dbName, tableName, 10);
        checkFlussTableData(data);
    }

    public void checkFlussTableData(List<GenericRow> streamData) {
        Assertions.assertEquals(3, streamData.size());
        List<String> expectedResult =
                Arrays.asList(
                        "([109, 105, 73, 90, 106],true,1940337748,73,17489,7408919466156976747,9.434991E37,3.140411637757371E307,4029933791018936000000.00000000,aaaaa,20091,9010000,2025-05-27T21:56:09,2025-09-27T18:54:08Z)",
                        "([109, 105, 73, 90, 106],true,90650390,37,22504,5851888708829345169,2.6221706E36,1.8915341983748786E307,3093109630614623000000.00000000,bbbbb,20089,76964000,2025-05-08T05:26:18,2025-08-04T08:49:45Z)",
                        "([109, 105, 73, 90, 106],true,388742243,89,15831,159071788675312856,7.310445E37,1.2166972324288247E308,7994947075691901000000.00000000,ddddd,20092,55687000,2025-07-18T08:59:49,2025-09-12T15:46:25Z)");
        ArrayList<String> result = new ArrayList<>();
        for (GenericRow streamDatum : streamData) {
            result.add(streamDatum.toString());
        }
        Assertions.assertEquals(expectedResult, result);
    }

    public void checkFlussTableStreamData(List<GenericRow> streamData) {
        Assertions.assertEquals(7, streamData.size());
        List<String> expectedResult =
                Arrays.asList(
                        "([109, 105, 73, 90, 106],true,1940337748,73,17489,7408919466156976747,9.434991E37,3.140411637757371E307,4029933791018936000000.00000000,aaaaa,20091,9010000,2025-05-27T21:56:09,2025-09-27T18:54:08Z)",
                        "([109, 105, 73, 90, 106],true,90650390,37,22504,5851888708829345169,2.6221706E36,1.8915341983748786E307,3093109630614623000000.00000000,bbbbb,20089,76964000,2025-05-08T05:26:18,2025-08-04T08:49:45Z)",
                        "([109, 105, 73, 90, 106],true,2146418323,79,19821,6393905306944584839,2.0462337E38,1.4868114385836557E308,5594947262031770000000.00000000,ccccc,20367,79840000,2025-03-25T01:49:14,2025-07-03T03:52:06Z)",
                        "([109, 105, 73, 90, 106],true,2146418323,79,19821,6393905306944584839,2.0462337E38,1.4868114385836557E308,5594947262031770000000.00000000,ccccc,20367,79840000,2025-03-25T01:49:14,2025-07-03T03:52:06Z)",
                        "([109, 105, 73, 90, 106],true,82794384,27,30339,5826566947079347516,2.2137477E37,1.7737681870839753E308,3984670873242882300000.00000000,ddddd,20344,37972000,2025-01-27T19:20:51,2025-11-06T18:38:54Z)",
                        "([109, 105, 73, 90, 106],true,82794384,27,30339,5826566947079347516,2.2137477E37,1.7737681870839753E308,3984670873242882300000.00000000,ddddd,20344,37972000,2025-01-27T19:20:51,2025-11-06T18:38:54Z)",
                        "([109, 105, 73, 90, 106],true,388742243,89,15831,159071788675312856,7.310445E37,1.2166972324288247E308,7994947075691901000000.00000000,ddddd,20092,55687000,2025-07-18T08:59:49,2025-09-12T15:46:25Z)");
        ArrayList<String> result = new ArrayList<>();
        for (GenericRow streamDatum : streamData) {
            result.add(streamDatum.toString());
        }
        Assertions.assertEquals(expectedResult, result);
    }

    public List<GenericRow> getFlussTableStreamData(
            Connection connection, String dbName, String tableName, int scanNum) {
        TablePath tablePath = TablePath.of(dbName, tableName);
        Table table = connection.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }
        int scanned = 0;
        List<GenericRow> rows = new ArrayList<>();

        while (true) {
            if (scanned > scanNum) break;
            log.info("Polling for stream records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                for (ScanRecord record : scanRecords.records(bucket)) {
                    GenericRow row = (GenericRow) record.getRow();
                    rows.add(row);
                }
            }
            scanned++;
        }
        return rows;
    }

    public List<GenericRow> getFlussTableData(
            Connection connection, String dbName, String tableName, int scanNum)
            throws IOException {
        TablePath tablePath = TablePath.of(dbName, tableName);
        Table table = connection.getTable(tablePath);
        LogScanner logScanner = table.newScan().createLogScanner();
        int numBuckets = table.getTableInfo().getNumBuckets();
        for (int i = 0; i < numBuckets; i++) {
            logScanner.subscribeFromBeginning(i);
        }
        int scanned = 0;
        List<GenericRow> rows = new ArrayList<>();

        while (true) {
            if (scanned > scanNum) break;
            log.info("Polling for records...");
            ScanRecords scanRecords = logScanner.poll(Duration.ofSeconds(1));
            for (TableBucket bucket : scanRecords.buckets()) {
                CloseableIterator<InternalRow> data =
                        table.newScan()
                                .limit(10)
                                .createBatchScanner(bucket)
                                .pollBatch(Duration.ofSeconds(5));
                while (data.hasNext()) {
                    rows.add((GenericRow) data.next());
                }
            }
            scanned++;
        }
        return rows;
    }
}
