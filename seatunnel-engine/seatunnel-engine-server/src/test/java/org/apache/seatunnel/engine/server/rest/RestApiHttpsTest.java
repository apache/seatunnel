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

package org.apache.seatunnel.engine.server.rest;

import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.*;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.task.CoordinatorTask;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** JobMaster Tester. */
@DisabledOnOs(OS.WINDOWS)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RestApiHttpsTest extends AbstractSeaTunnelServerTest {
    /**
     * IMap key is jobId and value is a Tuple2 Tuple2 key is JobMaster init timestamp and value is
     * the jobImmutableInformation which is sent by client when submit job
     *
     * <p>This IMap is used to recovery runningJobInfoIMap in JobMaster when a new master node
     * active
     */
    private IMap<Long, JobInfo> runningJobInfoIMap;

    /**
     * IMap key is one of jobId {@link
     * PipelineLocation} and {@link
     * TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link JobStatus} {@link PipelineStatus} {@link
     * org.apache.seatunnel.engine.server.execution.ExecutionState}
     *
     * <p>This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node
     * active
     */
    IMap<Object, Object> runningJobStateIMap;

    /**
     * IMap key is one of jobId {@link
     * PipelineLocation} and {@link
     * TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan} stateTimestamps {@link
     * SubPlan} stateTimestamps {@link
     * PhysicalVertex} stateTimestamps
     *
     * <p>This IMap is used to recovery runningJobStateTimestampsIMap in JobMaster when a new master
     * node active
     */
    IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * IMap key is {@link PipelineLocation}
     *
     * <p>The value of IMap is map of {@link TaskGroupLocation} and the {@link SlotProfile} it used.
     *
     * <p>This IMap is used to recovery ownedSlotProfilesIMap in JobMaster when a new master node
     * active
     */
    private IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    @BeforeAll
    public void before() {
        String name = this.getClass().getName();
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(
                TestUtils.getClusterName("AbstractSeaTunnelServerTest_" + name));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);
        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setHttpsPort(8443);
        httpConfig.setEnableHttps(true);
        httpConfig.setKeyStorePath("/home/jast/IdeaProjects/data-integration/seatunnel-examples/seatunnel-engine-examples/server_keystore.jks");
        httpConfig.setTrustStorePath("/home/jast/IdeaProjects/data-integration/seatunnel-examples/seatunnel-engine-examples/server_truststore.jks");
        httpConfig.setKeyManagerPassword("server_keystore_password");
        httpConfig.setKeyStorePassword("server_keystore_password");
        httpConfig.setTrustStorePassword("server_truststore_password");
        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    @Test
    public void testHandleCheckpointTimeout() throws Exception {

        String keystorePath = "/home/jast/IdeaProjects/data-integration/seatunnel-examples/seatunnel-engine-examples/client.p12";
        String keystorePass = "client_keystore_password";
        String truststorePath = "/home/jast/IdeaProjects/data-integration/seatunnel-examples/seatunnel-engine-examples/server.crt.pem";

        try {
            KeyStore clientStore = KeyStore.getInstance("PKCS12");
            clientStore.load(new FileInputStream(keystorePath), keystorePass.toCharArray());

            KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            kmf.init(clientStore, keystorePass.toCharArray());

            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            X509Certificate serverCert = (X509Certificate) cf.generateCertificate(
                    new BufferedInputStream(new FileInputStream(truststorePath)));

            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            trustStore.load(null, null);
            trustStore.setCertificateEntry("server-cert", serverCert);

            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);

            java.net.URL url = new java.net.URL("https://localhost:8443/overview");
            HttpsURLConnection conn = (HttpsURLConnection) url.openConnection();
            conn.setSSLSocketFactory(sslContext.getSocketFactory());

            Assertions.assertEquals(200, conn.getResponseCode());

            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            String inputLine;
            StringBuilder response = new StringBuilder();
            while ((inputLine = in.readLine()) != null) {
                response.append(inputLine);
            }
            in.close();

            Assertions.assertTrue(response.toString().contains("projectVersion"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
