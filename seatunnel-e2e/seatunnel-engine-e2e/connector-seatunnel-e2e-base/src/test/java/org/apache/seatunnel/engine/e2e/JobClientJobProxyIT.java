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

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

public class JobClientJobProxyIT extends SeaTunnelEngineContainer {

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        // use seatunnel_fixed_slot_num.yaml replace seatunnel.yaml in container
        this.server =
                createSeaTunnelContainerWithFakeSourceAndInMemorySink(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel_fixed_slot_num.yaml");
    }

    @Test
    public void testJobRetryTimes() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/retry-times/stream_fake_to_inmemory_with_error_retry_1.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                server.getLogs()
                        .contains(
                                "Restore time 1, pipeline Job stream_fake_to_inmemory_with_error_retry_1.conf"));
        Assertions.assertFalse(
                server.getLogs()
                        .contains(
                                "Restore time 3, pipeline Job stream_fake_to_inmemory_with_error_retry_1.conf"));

        Container.ExecResult execResult2 =
                executeJob(server, "/retry-times/stream_fake_to_inmemory_with_error.conf");
        Assertions.assertNotEquals(0, execResult2.getExitCode());
        Assertions.assertTrue(
                server.getLogs()
                        .contains(
                                "Restore time 3, pipeline Job stream_fake_to_inmemory_with_error.conf"),
                server.getLogs());
    }

    @Test
    public void testNoDuplicatedReleaseSlot() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/savemode/fake_to_inmemory_savemode.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
        Assertions.assertFalse(
                server.getLogs().contains("wrong target release operation with job"));
    }

    @Test
    public void testMultiTableSinkFailedWithThrowable() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/stream_fake_to_inmemory_with_throwable_error.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                execResult.getStderr().contains("table fake sink throw error"),
                execResult.getStderr());
    }

    @Test
    public void testSaveModeOnMasterOrClient() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/savemode/fake_to_inmemory_savemode.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        int serverLogLength = 0;
        String serverLogs = server.getLogs();
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle schema savemode with table path: test.table1"));
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle data savemode with table path: test.table1"));
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle schema savemode with table path: test.table2"));
        Assertions.assertTrue(
                serverLogs.contains(
                        "org.apache.seatunnel.e2e.sink.inmemory.InMemorySaveModeHandler - handle data savemode with table path: test.table2"));

        // restore will not execute savemode
        execResult = restoreJob(server, "/savemode/fake_to_inmemory_savemode.conf", "1", null);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        // clear old logs
        serverLogLength += serverLogs.length();
        serverLogs = server.getLogs().substring(serverLogLength);
        Assertions.assertFalse(serverLogs.contains("handle schema savemode with table path"));
        Assertions.assertFalse(serverLogs.contains("handle data savemode with table path"));

        // test savemode on client side
        Container.ExecResult execResult2 =
                executeJob(server, "/savemode/fake_to_inmemory_savemode_client.conf");
        Assertions.assertEquals(0, execResult2.getExitCode(), execResult2.getStderr());
        // clear old logs
        serverLogLength += serverLogs.length();
        serverLogs = server.getLogs().substring(serverLogLength);
        Assertions.assertFalse(serverLogs.contains("handle schema savemode with table path"));
        Assertions.assertFalse(serverLogs.contains("handle data savemode with table path"));

        Assertions.assertTrue(
                execResult2.getStdout().contains("handle schema savemode with table path"));
        Assertions.assertTrue(
                execResult2.getStdout().contains("handle data savemode with table path"));
    }

    @Test
    public void testJobFailedWillThrowException() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/batch_slot_not_enough.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                StringUtils.isNotBlank(execResult.getStderr())
                        && execResult
                                .getStderr()
                                .contains(
                                        "org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException"));
    }
}
