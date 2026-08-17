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

package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK},
        disabledReason = "Only support for seatunnel")
@DisabledOnOs(OS.WINDOWS)
@Slf4j
public class SeaTunnelConnectorBatchCancelTest extends TestSuiteBase implements TestResource {

    @Override
    public void startUp() throws Exception {}

    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void task(TestContainer container) throws IOException, InterruptedException {
        // Start test task
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/batch_cancel_task_1.conf");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });
        CompletableFuture.supplyAsync(
                () -> {
                    try {
                        container.executeJob("/batch_cancel_task_2.conf");
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    return null;
                });

        Awaitility.await()
                .atMost(5, TimeUnit.MINUTES)
                .pollInterval(1, TimeUnit.SECONDS)
                .until(() -> getRunningJobIds(container).size() == 2);
        List<String> runningJobId = getRunningJobIds(container);

        // Verify that the status is Running
        for (String jobId : runningJobId) {
            Container.ExecResult execResult1 =
                    container.executeBaseCommand(new String[] {"-j", jobId});
            String stdout = execResult1.getStdout();
            ObjectNode jsonNodes = JsonUtils.parseObject(stdout);
            Assertions.assertEquals(jsonNodes.get("jobStatus").asText(), "RUNNING");
        }

        // Execute batch cancellation tasks
        String[] batchCancelCommand =
                Stream.concat(Arrays.stream(new String[] {"-can"}), runningJobId.stream())
                        .toArray(String[]::new);
        Assertions.assertEquals(0, container.executeBaseCommand(batchCancelCommand).getExitCode());

        // Verify whether the cancellation is successful
        for (String jobId : runningJobId) {
            Awaitility.await()
                    .atMost(2, TimeUnit.MINUTES)
                    .pollInterval(1, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Container.ExecResult execResult =
                                        container.executeBaseCommand(new String[] {"-j", jobId});
                                ObjectNode jsonNodes =
                                        JsonUtils.parseObject(execResult.getStdout());
                                Assertions.assertEquals(
                                        "CANCELED", jsonNodes.get("jobStatus").asText());
                            });
        }
    }

    private List<String> getRunningJobIds(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeBaseCommand(new String[] {"-l"});
        Pattern pattern = Pattern.compile("(\\d+)\\s+");
        return Arrays.stream(execResult.getStdout().split("\n"))
                .filter(line -> line.contains("batch_cancel_task"))
                .map(
                        line -> {
                            Matcher matcher = pattern.matcher(line);
                            return matcher.find() ? matcher.group(1) : null;
                        })
                .filter(jobId -> jobId != null)
                .collect(Collectors.toList());
    }
}
