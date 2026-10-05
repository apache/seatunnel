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

package org.apache.seatunnel.edge.agent.e2e;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Container;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class AbstractEdgeAgentEngineIT extends EdgeAgentE2eTestBase
        implements TestResource {

    protected static final String AUTH_TOKEN = "edge-e2e-token";
    protected static final String E2E_SECRET_KEY = "dGVzdC1zZWNyZXQta2V5LTMyLWJ5dGVzLWFlczI1NiE=";

    protected abstract List<String> querySinkValues() throws Exception;

    protected void startSinkDependencies() throws Exception {}

    protected void stopSinkDependencies() throws Exception {}

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        startSinkDependencies();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        stopSinkDependencies();
    }

    protected void awaitSinkContainsExpectedMessages(List<String> expectedSubstrings) {
        if (expectedSubstrings == null || expectedSubstrings.isEmpty()) {
            throw new IllegalArgumentException("Expected substrings should not be empty");
        }
        Awaitility.await()
                .atMost(90, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<String> rows = querySinkValues();
                            for (String substring : expectedSubstrings) {
                                boolean found =
                                        rows.stream().anyMatch(row -> row.contains(substring));
                                Assertions.assertTrue(
                                        found,
                                        () ->
                                                "Missing expected substring in sink table: "
                                                        + substring
                                                        + ", actual rows: "
                                                        + rows);
                            }
                        });
    }

    protected Container.ExecResult waitForJobResult(
            CompletableFuture<Container.ExecResult> jobFuture) throws Exception {
        try {
            return jobFuture.get(180, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new AssertionError("Job command failed", cause);
        } catch (TimeoutException e) {
            throw new AssertionError("Timed out waiting for job command to finish", e);
        }
    }

    protected void assertJobRunningOrSubmissionFailed(
            TestContainer container,
            String jobId,
            CompletableFuture<Container.ExecResult> jobFuture)
            throws Exception {
        if (jobFuture.isDone()) {
            try {
                Container.ExecResult submitResult = jobFuture.get();
                Assertions.assertEquals(
                        0,
                        submitResult.getExitCode(),
                        "Submit job failed before reaching RUNNING: " + submitResult.getStderr());
            } catch (ExecutionException e) {
                throw new AssertionError("Submit job failed before reaching RUNNING", e.getCause());
            }
            Assertions.fail("Submit command exited before job reached RUNNING status");
        }
        Assertions.assertEquals(
                "RUNNING",
                container.getJobStatus(jobId),
                "EdgeSocket source job should be running before agent sends data");
    }

    protected List<String> buildPlainTextMessages(int count, String prefix) {
        List<String> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            messages.add(prefix + "-" + i);
        }
        return messages;
    }

    protected List<String> buildSchemaPayloadJsonMessages(
            int count, LinkedHashMap<String, String> schemaDefinition) {
        List<String> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            int fieldIndex = 0;
            for (Map.Entry<String, String> field : schemaDefinition.entrySet()) {
                if (fieldIndex++ > 0) {
                    builder.append(",");
                }
                builder.append("\"").append(escapeJson(field.getKey())).append("\":");
                builder.append(buildTypedJsonValue(field.getValue(), i));
            }
            builder.append("}");
            messages.add(builder.toString());
        }
        return messages;
    }

    private String buildTypedJsonValue(String fieldType, int index) {
        String normalizedType = fieldType == null ? "" : fieldType.trim().toLowerCase();
        if ("string".equals(normalizedType)) {
            return "\"value-" + index + "\"";
        }
        if ("int".equals(normalizedType)) {
            return String.valueOf(index);
        }
        if ("long".equals(normalizedType)) {
            return String.valueOf(index * 1000L);
        }
        if ("double".equals(normalizedType)) {
            return String.valueOf(index + 0.5D);
        }
        if ("boolean".equals(normalizedType)) {
            return String.valueOf(index % 2 == 0);
        }
        throw new IllegalArgumentException("Unsupported schema type: " + fieldType);
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
