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

package org.apache.seatunnel.e2e.flink.v2.tikv;

import static org.awaitility.Awaitility.given;

import org.apache.seatunnel.connectors.seatunnel.tikv.config.ClientSession;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVDataType;
import org.apache.seatunnel.connectors.seatunnel.tikv.config.TiKVParameters;
import org.apache.seatunnel.e2e.flink.FlinkContainer;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.tikv.raw.RawKVClient;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@Slf4j
public class TiKVIT extends FlinkContainer {

    private static final String HOST = "localhost";
    // tikv client Access the PD node directly, so the actual request port is 2379
    private static final int TIDB_PORT = 4000;
    private static final int PD_PORT = 2379;
    private static final int TIKV_PORT = 20160;
    // tidb need three containers refer to https://docs.pingcap.com/zh/tidb/v3.1/test-deployment-using-docker
    // tidb
    private static final String TIDB_CONTAINER_HOST = "flink_e2e_tidb";
    private static final String PINGCAP_TIDB_IMAGE = "pingcap/tidb:latest";
    // pd
    private static final String PD_CONTAINER_HOST = "flink_e2e_pd";
    private static final String PINGCAP_PD_IMAGE = "pingcap/pd:latest";
    // tidb
    private static final String TIKV_CONTAINER_HOST = "flink_e2e_tikv";
    private static final String PINGCAP_TIKV_IMAGE = "pingcap/tikv:latest";
    private GenericContainer<?> tidbContainer;
    private GenericContainer<?> pdContainer;
    private GenericContainer<?> tikvContainer;
    private ClientSession clientSession;
    private RawKVClient client;

    /**
     * tidb starting sequence ==> Placement Driver (PD) -> TiKV -> TiDB
     */
    @BeforeEach
    public void startTidbContainer() {
        // pd
        pdContainer = new GenericContainer<>(PINGCAP_PD_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(PD_CONTAINER_HOST)
            .withLogConsumer(new Slf4jLogConsumer(log));
        pdContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", PD_PORT, PD_PORT)));
        Startables.deepStart(Stream.of(pdContainer)).join();
        log.info("pd container started");

        // tikv
        tikvContainer = new GenericContainer<>(PINGCAP_TIKV_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(TIKV_CONTAINER_HOST)
            .withLogConsumer(new Slf4jLogConsumer(log));
        tikvContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", TIKV_PORT, TIKV_PORT)));
        Startables.deepStart(Stream.of(tikvContainer)).join();
        log.info("tikv container started");

        // tidb
        tidbContainer = new GenericContainer<>(PINGCAP_TIDB_IMAGE)
            .withNetwork(NETWORK)
            .withNetworkAliases(TIDB_CONTAINER_HOST)
            .withLogConsumer(new Slf4jLogConsumer(log));
        tidbContainer.setPortBindings(Lists.newArrayList(String.format("%s:%s", TIDB_PORT, TIDB_PORT)));
        Startables.deepStart(Stream.of(tidbContainer)).join();
        log.info("Tidb container started");

        given().ignoreExceptions()
            .await()
            .atLeast(100, TimeUnit.MILLISECONDS)
            .pollInterval(500, TimeUnit.MILLISECONDS)
            .atMost(180, TimeUnit.SECONDS)
            .untilAsserted(this::initClientSession);
        this.generateTestData();
    }

    private void initClientSession() {
        // init tikvParameters
        TiKVParameters tikvParameters = new TiKVParameters();
        tikvParameters.setHost(HOST);
        tikvParameters.setPdPort(PD_PORT);
        tikvParameters.setPdAddresses(HOST + ":" + PD_PORT);
        tikvParameters.setKeyField("k1");
        tikvParameters.setTikvDataType(TiKVDataType.KEY);
        tikvParameters.setLimit(1000);

        this.clientSession = new ClientSession(tikvParameters);
        this.client = this.clientSession.session.createRawClient();
    }

    private void generateTestData() {
        log.info("----------- put--------------");
        client.put(ByteString.copyFromUtf8("k1"), ByteString.copyFromUtf8("Hello"));
        client.put(ByteString.copyFromUtf8("k2"), ByteString.copyFromUtf8(","));
        client.put(ByteString.copyFromUtf8("k3"), ByteString.copyFromUtf8("World"));
        client.put(ByteString.copyFromUtf8("k4"), ByteString.copyFromUtf8("!"));
        client.put(ByteString.copyFromUtf8("k5"), ByteString.copyFromUtf8("Raw KV"));
    }

    @Test
    public void testRedisSourceAndSink() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelFlinkJob("/tikv/tikv_source_and_sink.conf");
        // get
        log.info("----------- get--------------");
        Optional<ByteString> result = client.get(ByteString.copyFromUtf8("k1"));
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertEquals("Hello", result.get().toStringUtf8());
    }

    @AfterEach
    public void close() {
        try {
            clientSession.close();
            tidbContainer.close();
            pdContainer.close();
            tikvContainer.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
