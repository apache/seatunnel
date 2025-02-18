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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.e2e.DummySSLUtil.DummySSLStores;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.restassured.RestAssured.given;
import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.hamcrest.Matchers.containsString;

@Slf4j
public class RestApiHttpsIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy clientJobProxy;
    private static ClientJobProxy batchJobProxy;
    private static HazelcastInstanceImpl node1;
    private static HazelcastInstanceImpl node2;
    private static SeaTunnelClient engineClient;
    private static SeaTunnelConfig node1Config;
    private static SeaTunnelConfig node2Config;
    private static Map<Integer, Integer> ports;

    // Dummy SSL stores for the server and client.
    private static DummySSLStores serverStores;
    private static DummySSLStores clientStores;

    @BeforeEach
    void beforeClass() throws Exception {
        // Set up logging configuration.
        LoggerContext context = (LoggerContext) LogManager.getContext(false);
        context.setConfigLocation(
                Paths.get(
                                PROJECT_ROOT_PATH
                                        + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/job-log-file/log4j2.properties")
                        .toUri());

        String testClusterName = TestUtils.getClusterName("RestApiHttpsIT");
        node1Config = ConfigProvider.locateAndGetSeaTunnelConfig();
        node1Config.getHazelcastConfig().setClusterName(testClusterName);
        node1Config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);

        // Generate dummy SSL stores.
        serverStores = DummySSLUtil.generateDummySSLStores();
        clientStores = DummySSLUtil.generateDummySSLStores();

        // Configure node1 for HTTPS with two‑way SSL.
        node1Config.getEngineConfig().getHttpConfig().setHttpsPort(9443);
        node1Config.getEngineConfig().getHttpConfig().setEnableHttps(true);
        node1Config.getEngineConfig().getHttpConfig().setKeystore(serverStores.keystorePath);
        node1Config.getEngineConfig().getHttpConfig().setKeystorePassword("changeit");
        node1Config.getEngineConfig().getHttpConfig().setKeyPassword("changeit");
        // Set truststore so that the server trusts client certificates.
        node1Config.getEngineConfig().getHttpConfig().setTruststore(clientStores.truststorePath);
        node1Config.getEngineConfig().getHttpConfig().setTruststorePassword("changeit");

        node1Config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        MemberAttributeConfig node1Tags = new MemberAttributeConfig();
        node1Tags.setAttribute("node", "node1");
        node1Config.getHazelcastConfig().setMemberAttributeConfig(node1Tags);
        node1 = SeaTunnelServerStarter.createHazelcastInstance(node1Config);

        // Configure node2 similarly.
        MemberAttributeConfig node2Tags = new MemberAttributeConfig();
        node2Tags.setAttribute("node", "node2");
        Config node2hzconfig = node1Config.getHazelcastConfig().setMemberAttributeConfig(node2Tags);
        node2Config = ConfigProvider.locateAndGetSeaTunnelConfig();
        node2Config
                .getEngineConfig()
                .getHttpConfig()
                .setHttpsPort(9443); // or use a different port if desired
        node2Config.getEngineConfig().getHttpConfig().setEnableHttps(true);
        node2Config.getEngineConfig().getHttpConfig().setKeystore(serverStores.keystorePath);
        node2Config.getEngineConfig().getHttpConfig().setKeystorePassword("changeit");
        node2Config.getEngineConfig().getHttpConfig().setKeyPassword("changeit");
        node2Config.getEngineConfig().getHttpConfig().setTruststore(clientStores.truststorePath);
        node2Config.getEngineConfig().getHttpConfig().setTruststorePassword("changeit");
        node2Config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        node2Config.setHazelcastConfig(node2hzconfig);
        node2 = SeaTunnelServerStarter.createHazelcastInstance(node2Config);

        // Submit a streaming job.
        String filePath = TestUtils.getResource("stream_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");
        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, node1Config);
        clientJobProxy = jobExecutionEnv.execute();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING, clientJobProxy.getJobStatus()));

        // Submit a batch job.
        String batchFilePath = TestUtils.getResource("fakesource_to_console.conf");
        JobConfig batchConf = new JobConfig();
        batchConf.setName("fake_to_console");
        ClientJobExecutionEnvironment batchJobExecutionEnv =
                engineClient.createExecutionContext(batchFilePath, batchConf, node1Config);
        batchJobProxy = batchJobExecutionEnv.execute();
        Awaitility.await()
                .atMost(5, TimeUnit.MINUTES)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.FINISHED, batchJobProxy.getJobStatus()));

        // Map Hazelcast member ports to HTTP ports.
        ports = new HashMap<>();
        ports.put(
                node1.getCluster().getLocalMember().getAddress().getPort(),
                node1Config.getEngineConfig().getHttpConfig().getHttpsPort());
        ports.put(
                node2.getCluster().getLocalMember().getAddress().getPort(),
                node2Config.getEngineConfig().getHttpConfig().getHttpsPort());
    }

    // One-way SSL: client does not present its certificate.
    @Test
    public void testHttpsOneWayAuthentication() {
        ports.forEach(
                (memberPort, httpPort) -> {
                    given().relaxedHTTPSValidation() // Allow self-signed certs.
                            .when()
                            .get(
                                    "https://localhost:"
                                            + httpPort
                                            + node1Config
                                                    .getEngineConfig()
                                                    .getHttpConfig()
                                                    .getContextPath())
                            .then()
                            .statusCode(200)
                            .contentType(containsString("text/html"))
                            .body(containsString("<html"))
                            .body(containsString("<title>Seatunnel Engine UI</title>"));
                });
    }

    // Two-way SSL: client presents its certificate.
    @Test
    public void testHttpsTwoWayAuthenticationSuccess() {
        ports.forEach(
                (memberPort, httpPort) -> {
                    given().keyStore(
                                    clientStores.keystorePath,
                                    "changeit") // Present client's certificate.
                            .relaxedHTTPSValidation()
                            .when()
                            .get(
                                    "https://localhost:"
                                            + httpPort
                                            + node1Config
                                                    .getEngineConfig()
                                                    .getHttpConfig()
                                                    .getContextPath())
                            .then()
                            .statusCode(200)
                            .contentType(containsString("text/html"))
                            .body(containsString("<html"))
                            .body(containsString("<title>Seatunnel Engine UI</title>"));
                });
    }

    @AfterEach
    void afterClass() {
        if (engineClient != null) {
            engineClient.close();
        }
        if (node1 != null) {
            node1.shutdown();
        }
        if (node2 != null) {
            node2.shutdown();
        }
    }
}
