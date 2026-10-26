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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.JettyService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.ServerSocket;
import java.net.URL;

public class GetNodeHttpPortOperationTest
        extends AbstractSeaTunnelServerTest<GetNodeHttpPortOperationTest> {

    private static final int HTTP_PORT = TestUtils.getAvailablePort(100);

    @BeforeAll
    @Override
    public void before() {
        // Keep the configured port occupied until Jetty has selected and bound another port.
        try (ServerSocket occupied = new ServerSocket(HTTP_PORT)) {
            super.before();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig config = super.loadSeaTunnelConfig();
        config.getEngineConfig().setHttpConfig(new HttpConfig());
        config.getEngineConfig().getHttpConfig().setPort(HTTP_PORT);
        config.getEngineConfig().getHttpConfig().setEnabled(true);
        config.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);
        return config;
    }

    @Test
    public void testReturnsBoundHttpPortWithoutChangingConfiguration() throws Exception {
        Address localAddress = instance.getCluster().getLocalMember().getAddress();

        int result =
                (int)
                        NodeEngineUtil.sendOperationToMemberNode(
                                        nodeEngine, new GetNodeHttpPortOperation(), localAddress)
                                .get();

        Assertions.assertNotEquals(
                HTTP_PORT, result, "The dynamically bound port must be published to peers");
        Assertions.assertEquals(server.getHttpPort(), result);
        Assertions.assertEquals(
                HTTP_PORT, server.getSeaTunnelConfig().getEngineConfig().getHttpConfig().getPort());
    }

    @Test
    public void testConfiguredPortFallbackBeforeStartup() {
        SeaTunnelConfig config = new SeaTunnelConfig();
        config.getEngineConfig().setHttpConfig(new HttpConfig());
        config.getEngineConfig().getHttpConfig().setPort(18085);
        Assertions.assertEquals(18085, new SeaTunnelServer(config).getHttpPort());
    }

    @Test
    public void testHttpsOnlyDoesNotProbeTheDisabledHttpPort() throws Exception {
        try (ServerSocket occupied = new ServerSocket(0)) {
            SeaTunnelConfig config = new SeaTunnelConfig();
            config.getEngineConfig().setHttpConfig(new HttpConfig());
            config.getEngineConfig().getHttpConfig().setPort(occupied.getLocalPort());
            config.getEngineConfig().getHttpConfig().setPortRange(0);
            config.getEngineConfig().getHttpConfig().setEnableDynamicPort(true);
            config.getEngineConfig().getHttpConfig().setEnabled(false);
            config.getEngineConfig().getHttpConfig().setEnableHttps(true);
            URL keyStore = getClass().getClassLoader().getResource("https/server_keystore.jks");
            Assertions.assertNotNull(keyStore);
            config.getEngineConfig().getHttpConfig().setKeyStorePath(keyStore.toExternalForm());
            // An omitted password makes Jetty read from Surefire's command stream on stdin.
            config.getEngineConfig()
                    .getHttpConfig()
                    .setKeyStorePassword("server_keystore_password");
            config.getEngineConfig()
                    .getHttpConfig()
                    .setKeyManagerPassword("server_keystore_password");

            JettyService service = new JettyService(instance.node.getNodeEngine(), config);
            Assertions.assertEquals(occupied.getLocalPort(), service.getHttpPort());
        }
    }
}
