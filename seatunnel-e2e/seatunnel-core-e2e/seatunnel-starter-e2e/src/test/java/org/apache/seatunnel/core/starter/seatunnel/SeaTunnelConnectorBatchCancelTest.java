package org.apache.seatunnel.core.starter.seatunnel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

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

        // Wait for the task to start
        Thread.sleep(15000);

        // Get the task id
        Container.ExecResult execResult = container.executeBaseCommand(new String[] {"-l"});
        String regex = "(\\d+)\\s+";
        Pattern pattern = Pattern.compile(regex);
        List<String> runningJobId =
                Arrays.stream(execResult.getStdout().toString().split("\n"))
                        .filter(s -> s.contains("batch_cancel_task"))
                        .map(
                                s -> {
                                    Matcher matcher = pattern.matcher(s);
                                    return matcher.find() ? matcher.group(1) : null;
                                })
                        .filter(jobId -> jobId != null)
                        .collect(Collectors.toList());

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
            Container.ExecResult execResult1 =
                    container.executeBaseCommand(new String[] {"-j", jobId});
            String stdout = execResult1.getStdout();
            ObjectNode jsonNodes = JsonUtils.parseObject(stdout);
            Assertions.assertEquals(jsonNodes.get("jobStatus").asText(), "CANCELED");
        }
    }
}