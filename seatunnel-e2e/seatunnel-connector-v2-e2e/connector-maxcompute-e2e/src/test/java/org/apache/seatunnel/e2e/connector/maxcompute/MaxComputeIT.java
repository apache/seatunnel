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

package org.apache.seatunnel.e2e.connector.maxcompute;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.MaxcomputeSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerLoggerFactory;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.task.SQLTask;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class MaxComputeIT extends TestSuiteBase implements TestResource {

    private GenericContainer<?> maxcompute;

    private static final int HOST_PORT = 8080;
    private static final int LOCAL_PORT = 8180;

    private static final String IMAGE = "maxcompute/maxcompute-emulator:v0.0.7";

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        this.maxcompute =
                new GenericContainer<>(IMAGE)
                        .withExposedPorts(HOST_PORT)
                        .withNetwork(NETWORK)
                        .withNetworkAliases("maxcompute")
                        .waitingFor(
                                Wait.forLogMessage(
                                        ".*Started MaxcomputeEmulatorApplication.*\\n", 1))
                        .withLogConsumer(
                                new Slf4jLogConsumer(DockerLoggerFactory.getLogger(IMAGE)));
        maxcompute.setPortBindings(
                Lists.newArrayList(String.format("%s:%s", LOCAL_PORT, HOST_PORT)));

        Startables.deepStart(Stream.of(this.maxcompute)).join();
        log.info("MaxCompute container started");
        Awaitility.given()
                .ignoreExceptions()
                .await()
                .atMost(360L, TimeUnit.SECONDS)
                .untilAsserted(this::initConnection);
        initTable();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (this.maxcompute != null) {
            this.maxcompute.stop();
        }
    }

    public Odps getTestOdps() {
        Account account = new AliyunAccount("ak", "sk");
        Odps odps = new Odps(account);
        odps.setEndpoint(getEndpoint(LOCAL_PORT));
        odps.setDefaultProject("mocked_mc");
        odps.setTunnelEndpoint(getEndpoint(LOCAL_PORT));
        return odps;
    }

    private void initConnection() throws OdpsException {
        Odps odps = getTestOdps();
        Assertions.assertFalse(odps.tables().exists("test_table"));
    }

    private void initTable() throws Exception {
        prepareLocal();

        Odps odps = getTestOdps();
        createTableWithData(odps, "test_table");
        createTableWithData(odps, "test_table_2");
        Assertions.assertTrue(odps.projects().exists("mocked_mc"));
        Assertions.assertTrue(odps.tables().exists("mocked_mc", "test_table"));
        Assertions.assertTrue(odps.tables().exists("mocked_mc", "test_table_2"));
    }

    private void prepareLocal() throws IOException {
        sendPOST(getEndpoint(LOCAL_PORT) + "/init", getEndpoint(LOCAL_PORT));
    }

    private void prepareContainer() throws IOException {
        sendPOST(getEndpoint(LOCAL_PORT) + "/init", getEndpoint(HOST_PORT));
    }

    private static void createTableWithData(Odps odps, String tableName) throws OdpsException {
        Instance instance =
                SQLTask.run(odps, "create table " + tableName + " (id INT, name STRING, age INT);");
        instance.waitForSuccess();
        Assertions.assertTrue(odps.tables().exists(tableName));
        Instance insert =
                SQLTask.run(
                        odps,
                        "insert into "
                                + tableName
                                + " values (1, 'test', 20), (2, 'test2', 30), (3, 'test3', 40);");
        insert.waitForSuccess();
        Assertions.assertEquals(3, queryTable(odps, tableName).size());
    }

    private static List<Record> queryTable(Odps odps, String tableName) throws OdpsException {
        Instance instance = SQLTask.run(odps, "select * from " + tableName + ";");
        instance.waitForSuccess();
        return SQLTask.getResult(instance);
    }

    private String getEndpoint(int port) {
        String ip;
        if (maxcompute.getHost().equals("localhost")) {
            try {
                ip = InetAddress.getLocalHost().getHostAddress();
            } catch (UnknownHostException e) {
                ip = "127.0.0.1";
            }
        } else {
            ip = maxcompute.getHost();
        }
        return "http://" + ip + ":" + port;
    }

    @TestTemplate
    @Disabled(
            "maxcompute-emulator does not support upload session for now, we need move to upsert session in MaxComputeWriter")
    public void testMaxCompute(TestContainer container)
            throws IOException, InterruptedException, OdpsException {
        Odps odps = getTestOdps();
        odps.tables().delete("mocked_mc", "test_table_sink", true);
        prepareContainer();
        Container.ExecResult execResult = container.executeJob("/maxcompute_to_maxcompute.conf");
        prepareLocal();
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(3, odps.tables().get("test_table_sink").getRecordNum());
        List<Record> records = queryTable(odps, "test_table_sink");
        Assertions.assertEquals(3, records.size());
        Assertions.assertEquals(1, records.get(0).get("id"));
        Assertions.assertEquals("test", records.get(0).get("name"));
        Assertions.assertEquals(20, records.get(0).get("age"));
        Assertions.assertEquals(2, records.get(1).get("id"));
        Assertions.assertEquals("test2", records.get(1).get("name"));
        Assertions.assertEquals(30, records.get(1).get("age"));
        Assertions.assertEquals(3, records.get(2).get("id"));
        Assertions.assertEquals("test3", records.get(2).get("name"));
        Assertions.assertEquals(40, records.get(2).get("age"));
    }

    @TestTemplate
    @Disabled(
            "maxcompute-emulator does not support upload session for now, we need move to upsert session in MaxComputeWriter")
    public void testMaxComputeMultiTable(TestContainer container)
            throws OdpsException, IOException, InterruptedException {
        Odps odps = getTestOdps();
        odps.tables().delete("mocked_mc", "test_table_sink", true);
        odps.tables().delete("mocked_mc", "test_table_2_sink", true);
        prepareContainer();
        Container.ExecResult execResult =
                container.executeJob("/maxcompute_to_maxcompute_multi_table.conf");
        prepareLocal();
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals(3, queryTable(odps, "test_table_sink").size());
        Assertions.assertEquals(3, queryTable(odps, "test_table_2_sink").size());
    }

    @Test
    public void testReadColumn() {
        Map<String, Object> config = new HashMap<>();
        config.put("accessId", "ak");
        config.put("accesskey", "sk");
        config.put("endpoint", getEndpoint(LOCAL_PORT));
        config.put("project", "mocked_mc");
        config.put("table_name", "test_table");
        config.put("read_columns", Arrays.asList("ID", "NAME"));
        SeaTunnelSource<Object, SourceSplit, Serializable> source =
                new MaxcomputeSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(config),
                                        Thread.currentThread().getContextClassLoader()))
                        .createSource();
        CatalogTable table = source.getProducedCatalogTables().get(0);
        Assertions.assertArrayEquals(
                new String[] {"ID", "NAME"}, table.getTableSchema().getFieldNames());
    }

    // here use java http client to send post, okhttp or other http client can also be used
    public static void sendPOST(String postUrl, String postData) throws IOException {
        URL url = new URL(postUrl);

        HttpURLConnection httpURLConnection = (HttpURLConnection) url.openConnection();
        httpURLConnection.setRequestMethod("POST");
        httpURLConnection.setDoOutput(true);
        httpURLConnection.setRequestProperty("Content-Type", "application/json");
        httpURLConnection.setRequestProperty("Content-Length", String.valueOf(postData.length()));

        try (OutputStream outputStream = httpURLConnection.getOutputStream()) {
            outputStream.write(postData.getBytes("UTF-8"));
            outputStream.flush();
        }
        int responseCode = httpURLConnection.getResponseCode();
        if (responseCode != HttpURLConnection.HTTP_OK) {
            throw new RuntimeException("POST request failed with response code: " + responseCode);
        }
    }
}
