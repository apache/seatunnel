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

package org.apache.seatunnel.e2e.connector.kudu;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.ColumnTypeAttributes;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.CreateTableOptions;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.ToxiproxyContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import org.testcontainers.utility.DockerLoggerFactory;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.String.format;
import static org.awaitility.Awaitility.await;

@Slf4j
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK},
        disabledReason = "Currently SPARK do not support cdc")
public class KuduCDCSinkIT extends TestSuiteBase implements TestResource {

    private static final String IMAGE = "apache/kudu:1.15.0";
    private static final Integer KUDU_MASTER_PORT = 7051;
    private static final Integer KUDU_TSERVER_PORT = 7053;
    private GenericContainer<?> master;
    private GenericContainer<?> tServers;
    private KuduClient kuduClient;

    private static final String TOXIPROXY_IMAGE = "ghcr.io/shopify/toxiproxy:2.4.0";
    private static final String TOXIPROXY_NETWORK_ALIAS = "toxiproxy";
    private ToxiproxyContainer toxiProxy;
    private String KUDU_SINK_TABLE = "kudu_sink_table";

    @BeforeAll
    @Override
    public void startUp() throws Exception {

        String hostIP = getHostIPAddress();

        this.master =
                new GenericContainer<>(IMAGE)
                        .withExposedPorts(KUDU_MASTER_PORT)
                        .withCommand("master")
                        .withEnv("MASTER_ARGS", "--default_num_replicas=1")
                        .withNetwork(NETWORK)
                        .withNetworkAliases("kudu-master-cdc")
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));

        toxiProxy =
                new ToxiproxyContainer(TOXIPROXY_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(TOXIPROXY_NETWORK_ALIAS);
        toxiProxy.start();

        String instanceName = "kudu-tserver-cdc";

        ToxiproxyContainer.ContainerProxy proxy =
                toxiProxy.getProxy(instanceName, KUDU_TSERVER_PORT);

        this.tServers =
                new GenericContainer<>(IMAGE)
                        .withExposedPorts(KUDU_TSERVER_PORT)
                        .withCommand("tserver")
                        .withEnv("KUDU_MASTERS", "kudu-master-cdc:" + KUDU_MASTER_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(instanceName)
                        .dependsOn(master)
                        .withEnv(
                                "TSERVER_ARGS",
                                format(
                                        "--fs_wal_dir=/var/lib/kudu/tserver --logtostderr --use_hybrid_clock=false --rpc_bind_addresses=%s:%s --rpc_advertised_addresses=%s:%s",
                                        instanceName,
                                        KUDU_TSERVER_PORT,
                                        hostIP,
                                        proxy.getProxyPort()))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));

        Startables.deepStart(Stream.of(master)).join();
        Startables.deepStart(Stream.of(tServers)).join();

        Awaitility.given()
                .ignoreExceptions()
                .atLeast(100, TimeUnit.MILLISECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .atMost(180, TimeUnit.SECONDS)
                .untilAsserted(this::getKuduClient);
    }

    private void initializeKuduTable() throws KuduException {

        List<ColumnSchema> columns = new ArrayList();

        columns.add(new ColumnSchema.ColumnSchemaBuilder("id", Type.INT32).key(true).build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_bool", Type.BOOL).nullable(true).build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_int8", Type.INT8).nullable(true).build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_int16", Type.INT16)
                        .nullable(true)
                        .build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_int32", Type.INT32)
                        .nullable(true)
                        .build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_int64", Type.INT64)
                        .nullable(true)
                        .build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_float", Type.FLOAT)
                        .nullable(true)
                        .build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_double", Type.DOUBLE)
                        .nullable(true)
                        .build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_decimal", Type.DECIMAL)
                        .nullable(true)
                        .typeAttributes(
                                new ColumnTypeAttributes.ColumnTypeAttributesBuilder()
                                        .precision(20)
                                        .scale(5)
                                        .build())
                        .build());
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_string", Type.STRING)
                        .nullable(true)
                        .build());
        // spark
        columns.add(
                new ColumnSchema.ColumnSchemaBuilder("val_unixtime_micros", Type.UNIXTIME_MICROS)
                        .nullable(true)
                        .build());

        Schema schema = new Schema(columns);

        ImmutableList<String> hashKeys = ImmutableList.of("id");
        CreateTableOptions tableOptions = new CreateTableOptions();

        tableOptions.addHashPartitions(hashKeys, 2);
        tableOptions.setNumReplicas(1);

        kuduClient.createTable(KUDU_SINK_TABLE, schema, tableOptions);
    }

    private void getKuduClient() {
        kuduClient =
                new AsyncKuduClient.AsyncKuduClientBuilder(
                                Arrays.asList(
                                        "127.0.0.1" + ":" + master.getMappedPort(KUDU_MASTER_PORT)))
                        .defaultAdminOperationTimeoutMs(120000)
                        .defaultOperationTimeoutMs(120000)
                        .build()
                        .syncClient();
    }

    @TestTemplate
    public void testKudu(TestContainer container) throws IOException, InterruptedException {
        this.initializeKuduTable();
        Container.ExecResult execResult = container.executeJob("/write-cdc-changelog-to-kudu.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        await().atMost(60000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertIterableEquals(
                                    Stream.<List<Object>>of(
                                                    Arrays.asList(
                                                            "3",
                                                            "true",
                                                            "1",
                                                            "2",
                                                            "3",
                                                            "4",
                                                            "4.3",
                                                            "5.3",
                                                            "6.30000",
                                                            "NEW",
                                                            "2020-02-02 02:02:02.0"),
                                                    Arrays.asList(
                                                            "1",
                                                            "true",
                                                            "2",
                                                            "2",
                                                            "3",
                                                            "4",
                                                            "4.3",
                                                            "5.3",
                                                            "6.30000",
                                                            "NEW",
                                                            "2020-02-02 02:02:02.0"))
                                            .collect(Collectors.toList()),
                                    readData(KUDU_SINK_TABLE));
                        });

        kuduClient.deleteTable(KUDU_SINK_TABLE);
    }

    public List<List<Object>> readData(String tableName) throws KuduException {
        List<List<Object>> result = new ArrayList<>();
        KuduTable kuduTable = kuduClient.openTable(tableName);
        KuduScanner scanner = kuduClient.newScannerBuilder(kuduTable).build();
        while (scanner.hasMoreRows()) {
            RowResultIterator rowResults = scanner.nextRows();
            List<Object> row = new ArrayList<>();
            while (rowResults.hasNext()) {
                RowResult rowResult = rowResults.next();
                for (int i = 0; i < rowResult.getSchema().getColumns().size(); i++) {
                    row.add(rowResult.getObject(i).toString());
                }
            }
            result.add(row);
        }
        return result;
    }

    @Override
    public void tearDown() throws Exception {
        if (kuduClient != null) {
            kuduClient.close();
        }

        if (master != null) {
            master.close();
        }

        if (tServers != null) {
            tServers.close();
        }
    }

    private static String getHostIPAddress() {
        try {
            Enumeration<NetworkInterface> networkInterfaceEnumeration =
                    NetworkInterface.getNetworkInterfaces();
            while (networkInterfaceEnumeration.hasMoreElements()) {
                for (InterfaceAddress interfaceAddress :
                        networkInterfaceEnumeration.nextElement().getInterfaceAddresses()) {
                    if (interfaceAddress.getAddress().isSiteLocalAddress()
                            && interfaceAddress.getAddress() instanceof Inet4Address) {
                        return interfaceAddress.getAddress().getHostAddress();
                    }
                }
            }
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }
        throw new IllegalStateException(
                "Could not find site local ipv4 address, failed to launch kudu");
    }
}
