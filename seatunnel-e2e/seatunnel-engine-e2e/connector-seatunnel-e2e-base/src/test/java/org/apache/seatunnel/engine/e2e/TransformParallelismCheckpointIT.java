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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.client.job.ClientJobExecutionEnvironment;
import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * A transform may declare a smaller parallelism than its source. It then runs chained into every
 * source subtask, each of which acknowledges checkpoints with its own index, so the checkpoint
 * state of the transform must cover all of them. Before that, the first checkpoint failed with an
 * index out of bounds: the job failed, or, with retries, was restarted and wrote rows twice.
 */
@Slf4j
public class TransformParallelismCheckpointIT {

    private static final String CLUSTER_NAME = "TransformParallelismCheckpointIT";

    @Test
    public void testTransformWithSmallerParallelismThanSource() throws Exception {
        String testCaseName = "testTransformWithSmallerParallelismThanSource";
        long rowsPerSubtask = 20;
        int sourceParallelism = 2;

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            Common.setDeployMode(DeployMode.CLIENT);

            String configFile =
                    String.join(File.separator, "", "tmp", "test_conf", testCaseName + ".conf");
            Map<String, String> values = new HashMap<>();
            values.put("dynamic_test_row_num_per_parallelism", String.valueOf(rowsPerSubtask));
            values.put("dynamic_test_parallelism", String.valueOf(sourceParallelism));
            TestUtils.createTestConfigFileFromTemplate(
                    "batch_fake_to_console_transform_parallelism_template.conf",
                    values,
                    configFile);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(configFile, jobConfig, seaTunnelConfig);
            ClientJobProxy clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> jobResult =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(jobResult.isDone()));
            Assertions.assertEquals(JobStatus.FINISHED, jobResult.get());

            // Every row is read and written exactly once.
            JsonNode metrics =
                    JsonUtils.parseObject(engineClient.getJobMetrics(clientJobProxy.getJobId()));
            Assertions.assertEquals(
                    rowsPerSubtask * sourceParallelism, sum(metrics, "SourceReceivedCount"));
            Assertions.assertEquals(
                    rowsPerSubtask * sourceParallelism, sum(metrics, "SinkWriteCount"));
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node != null) {
                node.shutdown();
            }
        }
    }

    /** Sums a job metric over the subtasks that report it. */
    private static long sum(JsonNode metrics, String name) {
        JsonNode values = metrics.get(name);
        Assertions.assertNotNull(values, name + " is missing from " + metrics);
        long total = 0;
        for (JsonNode value : values) {
            total += value.get("value").asLong();
        }
        return total;
    }
}
