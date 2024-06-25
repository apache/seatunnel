/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.e2e.resourceIsolation;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class ResourceIsolationIT extends TestSuiteBase {

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "only work on Zeta")
    public void testTagMatch(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/resource-isolation/fakesource_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK, EngineType.FLINK},
            disabledReason = "only work on Zeta")
    public void testTagNotMatch(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/resource-isolation/fakesource_to_console_tag_not_match.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        Assertions.assertTrue(
                StringUtils.isNotBlank(execResult.getStderr())
                        && execResult
                                .getStderr()
                                .contains(
                                        "org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException"));
    }
}
