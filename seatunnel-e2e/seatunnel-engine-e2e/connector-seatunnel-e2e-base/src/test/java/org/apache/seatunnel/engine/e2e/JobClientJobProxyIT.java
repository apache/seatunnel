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

public class JobClientJobProxyIT extends SeaTunnelContainer {

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
        Assertions.assertTrue(server.getLogs().contains("Restore time 1, pipeline"));
        Assertions.assertFalse(server.getLogs().contains("Restore time 3, pipeline"));

        Container.ExecResult execResult2 =
                executeJob(server, "/retry-times/stream_fake_to_inmemory_with_error.conf");
        Assertions.assertNotEquals(0, execResult2.getExitCode());
        Assertions.assertTrue(server.getLogs().contains("Restore time 3, pipeline"));
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
                                        "org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException: can't apply resource request"));
    }
}
