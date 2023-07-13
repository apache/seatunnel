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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.client.job.JobExecutionEnvironment;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryMetricConfig;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.rest.RestConstant;
import org.awaitility.Awaitility;
import org.hamcrest.Matcher;
import org.hamcrest.core.StringContains;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.matchesRegex;

@Slf4j
public class TelemetryApiIT {

    private static final String HOST = "http://localhost:";

    private static ClientJobProxy clientJobProxy;

    private static HazelcastInstanceImpl hazelcastInstance;

    private static TelemetryMetricConfig metricConfig;

    @BeforeAll
    static void beforeClass() throws Exception {
        String testClusterName = TestUtils.getClusterName("TelemetryApiIT");
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        // get TelemetryMetricConfig
        metricConfig = seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric();
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        // createTelemetryInstance
        SeaTunnelServerStarter.createTelemetryInstance(hazelcastInstance.node, seaTunnelConfig);
        Common.setDeployMode(DeployMode.CLIENT);
        String filePath = TestUtils.getResource("stream_fakesource_to_file.conf");
        JobConfig jobConfig = new JobConfig();
        jobConfig.setName("fake_to_file");

        ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
        clientConfig.setClusterName(testClusterName);
        SeaTunnelClient engineClient = new SeaTunnelClient(clientConfig);
        JobExecutionEnvironment jobExecutionEnv =
            engineClient.createExecutionContext(filePath, jobConfig);

        clientJobProxy = jobExecutionEnv.execute();

        Awaitility.await()
                  .atMost(2, TimeUnit.MINUTES)
                  .untilAsserted(
                      () ->
                          Assertions.assertEquals(
                              JobStatus.RUNNING, clientJobProxy.getJobStatus()));
    }

    @Test
    public void testGetMetrics() {
        given().get(
            HOST
                + metricConfig.getHttpPort()
                + "/metrics")
               .then()
               .statusCode(200)
               // Use regular expressions to verify whether the response body is the indicator data of Prometheus
               // Metric data is usually multi-line, use newlines for validation
               .body(matchesRegex("(?s)^.*# HELP.*# TYPE.*$"))
               // Verify that the response body contains a specific metric
               // JVM metrics
               .body(containsString("jvm_threads"))
               .body(containsString("jvm_memory_pool"))
               .body(containsString("jvm_gc"))
               .body(containsString("jvm_info"))
               .body(containsString("jvm_memory_bytes"))
               .body(containsString("jvm_classes"))
               .body(containsString("jvm_buffer_pool"))
               .body(containsString("process_start"))
               // Thread pool metrics
               .body(containsString("st_thread_pool_activeCount{address=\"[localhost]:5801\",}"))
               .body(containsString("st_thread_pool_completedTask_total{address=\"[localhost]:5801\",}"))
               .body(containsString("st_thread_pool_corePoolSize{address=\"[localhost]:5801\",}"))
               .body(containsString("st_thread_pool_maximumPoolSize{address=\"[localhost]:5801\",} 2.147483647E9"))
               .body(containsString("st_thread_pool_poolSize{address=\"[localhost]:5801\",}"))
               .body(containsString("st_thread_pool_task_total{address=\"[localhost]:5801\",}"))
               // Job count metrics
               .body(containsString("st_job_count{type=\"canceled\",} 0.0"))
               .body(containsString("st_job_count{type=\"cancelling\",} 0.0"))
               .body(containsString("st_job_count{type=\"created\",} 0.0"))
               .body(containsString("st_job_count{type=\"failed\",} 0.0"))
               .body(containsString("st_job_count{type=\"failing\",} 0.0"))
               .body(containsString("st_job_count{type=\"finished\",} 0.0"))
               .body(containsString("st_job_count{type=\"reconciling\",} 0.0"))
               .body(containsString("st_job_count{type=\"restarting\",} 0.0"))
               // Running job count is 1
               .body(containsString("st_job_count{type=\"running\",} 1.0"))
               .body(containsString("st_job_count{type=\"scheduled\",} 0.0"))
               .body(containsString("st_job_count{type=\"suspended\",} 0.0"));
    }

    @AfterAll
    static void afterClass() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }
}
