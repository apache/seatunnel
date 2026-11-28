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

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.config.DeployMode;
import org.apache.seatunnel.common.utils.FileUtils;
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
 * On Zeta a transform runs chained into the task of its source, so it runs once per source subtask
 * whatever {@code parallelism} it declares, and so does the sink behind it. These cases check that
 * such a job still checkpoints and commits the output of every subtask when the transform declares
 * a smaller or a larger parallelism than its source. The sink has an aggregated committer, which
 * counts the writers it waits for by the sink parallelism.
 *
 * <p>Before the fix for #13421 the smaller case failed at the first checkpoint with an {@code
 * ArrayIndexOutOfBoundsException}. The template sets {@code job.retry.times = 0}, so a job either
 * finishes or fails and is never restarted.
 */
@Slf4j
public class TransformParallelismCheckpointIT {

    private static final String CLUSTER_NAME = "TransformParallelismCheckpointIT";

    private static final long ROWS_PER_SUBTASK = 20;

    @Test
    public void testTransformWithSmallerParallelismThanSource() throws Exception {
        runAndCheckCommittedRows("testTransformWithSmallerParallelismThanSource", 2, 1);
    }

    @Test
    public void testTransformWithLargerParallelismThanSource() throws Exception {
        runAndCheckCommittedRows("testTransformWithLargerParallelismThanSource", 1, 2);
    }

    private void runAndCheckCommittedRows(
            String testCaseName, int sourceParallelism, int transformParallelism) throws Exception {
        String targetDir =
                String.join(File.separator, "", "tmp", "hive", "warehouse", testCaseName);
        FileUtils.createNewDir(targetDir);

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
        seaTunnelConfig.getEngineConfig().getHttpConfig().setEnabled(false);

        DeployMode previousDeployMode = Common.getDeployMode();
        HazelcastInstanceImpl node = null;
        SeaTunnelClient engineClient = null;
        ClientJobProxy clientJobProxy = null;
        try {
            node = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
            Common.setDeployMode(DeployMode.CLIENT);

            String configFile =
                    String.join(File.separator, "", "tmp", "test_conf", testCaseName + ".conf");
            Map<String, String> values = new HashMap<>();
            values.put("dynamic_test_case_name", testCaseName);
            values.put("dynamic_test_row_num_per_parallelism", String.valueOf(ROWS_PER_SUBTASK));
            values.put("dynamic_test_parallelism", String.valueOf(sourceParallelism));
            values.put("dynamic_transform_parallelism", String.valueOf(transformParallelism));
            TestUtils.createTestConfigFileFromTemplate(
                    "batch_fake_to_localfile_transform_parallelism_template.conf",
                    values,
                    configFile);

            JobConfig jobConfig = new JobConfig();
            jobConfig.setName(testCaseName);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(TestUtils.getClusterName(CLUSTER_NAME));
            engineClient = new SeaTunnelClient(clientConfig);
            ClientJobExecutionEnvironment jobExecutionEnv =
                    engineClient.createExecutionContext(configFile, jobConfig, seaTunnelConfig);
            clientJobProxy = jobExecutionEnv.execute();
            CompletableFuture<JobStatus> jobResult =
                    CompletableFuture.supplyAsync(clientJobProxy::waitForJobComplete);

            Awaitility.await()
                    .atMost(3, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertTrue(jobResult.isDone()));
            Assertions.assertEquals(JobStatus.FINISHED, jobResult.get());

            // Rows in the target directory are the committed output: a writer whose commit info
            // the aggregated committer drops leaves its rows out of it.
            Assertions.assertEquals(
                    ROWS_PER_SUBTASK * sourceParallelism,
                    FileUtils.getFileLineNumberFromDir(targetDir));
        } finally {
            try {
                if (clientJobProxy != null && !clientJobProxy.getJobStatus().isEndState()) {
                    clientJobProxy.cancelJob();
                }
            } finally {
                try {
                    if (engineClient != null) {
                        engineClient.close();
                    }
                    if (node != null) {
                        node.shutdown();
                    }
                } finally {
                    // The deploy mode is JVM-wide; restore it for the tests that share this JVM.
                    Common.setDeployMode(previousDeployMode);
                }
            }
        }
    }
}
