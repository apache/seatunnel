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
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static io.restassured.RestAssured.given;

/**
 * Regression test for UI "pipeline mixing" during refresh:
 *
 * <p>Repeated GET /job-info/{jobId} must return a stable DAG (pipelineEdges + vertex ids) within
 * the same running job.
 */
public class JobInfoDagStabilityRestIT {

    private static final String HOST = "http://localhost:";

    private HazelcastInstanceImpl node;

    private SeaTunnelClient engineClient;

    private SeaTunnelConfig config;

    private ClientJobProxy jobProxy;

    @BeforeEach
    void beforeEach() throws Exception {
        String testClusterName = TestUtils.getClusterName("JobInfoDagStabilityRestIT");
        config = ConfigProvider.locateAndGetSeaTunnelConfig();
        config.getEngineConfig().getHttpConfig().setPort(8080);
        config.getEngineConfig().getHttpConfig().setEnabled(true);
        config.getHazelcastConfig().setClusterName(testClusterName);
        config.getEngineConfig().getSlotServiceConfig().setDynamicSlot(false);
        config.getEngineConfig().getSlotServiceConfig().setSlotNum(20);
        MemberAttributeConfig tags = new MemberAttributeConfig();
        tags.setAttribute("node", "node1");
        config.getHazelcastConfig().setMemberAttributeConfig(tags);
        node = SeaTunnelServerStarter.createHazelcastInstance(config);

        String filePath =
                TestUtils.getResource("multi_pipeline_fake_to_console_observability.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("multi_pipeline_fake_to_console_observability");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        engineClient = new SeaTunnelClient(clientConfig);
        ClientJobExecutionEnvironment jobExecutionEnv =
                engineClient.createExecutionContext(filePath, jobConfig, config);

        jobProxy = jobExecutionEnv.execute();
        Awaitility.await()
                .atMost(2, TimeUnit.MINUTES)
                .untilAsserted(
                        () -> Assertions.assertEquals(JobStatus.RUNNING, jobProxy.getJobStatus()));
    }

    @AfterEach
    void afterEach() {
        if (engineClient != null) {
            engineClient.close();
        }
        if (node != null) {
            node.shutdown();
        }
    }

    @Test
    public void testJobInfoDagIsStableWithinOneRunningJob() {
        String baseUrl = HOST + config.getEngineConfig().getHttpConfig().getPort();
        long jobId = jobProxy.getJobId();

        String first = dagSignature(baseUrl, jobId);
        // Repeat calls (simulate UI refresh).
        for (int i = 0; i < 30; i++) {
            String next = dagSignature(baseUrl, jobId);
            Assertions.assertEquals(
                    first,
                    next,
                    "jobDag signature changed between requests, this may cause UI pipeline mixing");
        }
    }

    @SuppressWarnings("unchecked")
    private static String dagSignature(String baseUrl, long jobId) {
        Map<String, Object> pipelineEdges =
                given().get(baseUrl + "/job-info/" + jobId)
                        .then()
                        .statusCode(200)
                        .extract()
                        .path("jobDag.pipelineEdges");

        if (pipelineEdges == null || pipelineEdges.isEmpty()) {
            return "EMPTY";
        }

        List<String> parts = new ArrayList<>();
        for (Map.Entry<String, Object> entry : pipelineEdges.entrySet()) {
            String pipelineId = entry.getKey();
            Object edgesObj = entry.getValue();
            if (!(edgesObj instanceof List)) {
                parts.add(pipelineId + ":INVALID");
                continue;
            }
            List<Map<String, String>> edges = (List<Map<String, String>>) edgesObj;
            List<String> tuples =
                    edges.stream()
                            .map(e -> e.get("inputVertexId") + "->" + e.get("targetVertexId"))
                            .sorted()
                            .collect(Collectors.toList());
            parts.add(pipelineId + ":" + String.join(",", tuples));
        }

        Collections.sort(parts);
        return String.join("|", parts);
    }
}
