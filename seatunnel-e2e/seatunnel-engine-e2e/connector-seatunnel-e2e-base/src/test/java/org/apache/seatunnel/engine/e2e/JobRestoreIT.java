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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;

public class JobRestoreIT extends SeaTunnelEngineContainer {

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        this.server =
                createSeaTunnelContainerWithFakeSourceAndInMemorySink(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-seatunnel-e2e-base/src/test/resources/seatunnel_job_restore_apply_resources.yaml");
    }

    /** When testing job recovery, is it successful to reapply for resources */
    @Test
    public void testJobRestoreApplyResources() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeJob(server, "/restore-job/restore_job_apply_resources.conf");
        Assertions.assertEquals(1, execResult.getExitCode());
        Assertions.assertFalse(server.getLogs().contains("NoEnoughResourceException"));
    }
}
