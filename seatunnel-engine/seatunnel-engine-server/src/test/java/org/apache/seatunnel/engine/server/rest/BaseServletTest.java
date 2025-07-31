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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.internal.serialization.Data;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.Collections;

class BaseServletTest extends AbstractSeaTunnelServerTest {

    private static final int HTTP_PORT = 18080;

    private static final Long JOB_1 = System.currentTimeMillis() + 1L;

    @BeforeEach
    void setUp() {
        String name = this.getClass().getName();
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(TestUtils.getClusterName("RestApiServletTest_" + name));
        SeaTunnelConfig seaTunnelConfig = loadSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        seaTunnelConfig.getEngineConfig().setMode(ExecutionMode.LOCAL);

        HttpConfig httpConfig = seaTunnelConfig.getEngineConfig().getHttpConfig();
        httpConfig.setEnabled(true);
        httpConfig.setPort(HTTP_PORT);

        instance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        nodeEngine = instance.node.nodeEngine;
        server = nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
        LOGGER = nodeEngine.getLogger(AbstractSeaTunnelServerTest.class);
    }

    @Test
    void testWriteJsonWithObject() throws IOException {
        startJob(JOB_1, "fake_to_console.conf");
        testLogRestApiResponse("html");
        testLogRestApiResponse("JSON");
    }

    public void testLogRestApiResponse(String format) throws IOException {
        HttpURLConnection conn = null;
        try {
            java.net.URL url =
                    new java.net.URL("http://localhost:" + HTTP_PORT + "/logs?format=" + format);
            conn = (HttpURLConnection) url.openConnection();

            Assertions.assertEquals(200, conn.getResponseCode());
            Assertions.assertTrue(
                    conn.getHeaderFields()
                            .get("Content-Type")
                            .toString()
                            .contains("charset=utf-8"));
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }

    private void startJob(Long jobId, String path) {
        LogicalDag testLogicalDag = TestUtils.createTestLogicalPlan(path, jobId.toString(), jobId);

        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        nodeEngine.getSerializationService(),
                        testLogicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());

        Data data = nodeEngine.getSerializationService().toData(jobImmutableInformation);

        PassiveCompletableFuture<Void> voidPassiveCompletableFuture =
                server.getCoordinatorService()
                        .submitJob(jobId, data, jobImmutableInformation.isStartWithSavePoint());
        voidPassiveCompletableFuture.join();
    }
}
