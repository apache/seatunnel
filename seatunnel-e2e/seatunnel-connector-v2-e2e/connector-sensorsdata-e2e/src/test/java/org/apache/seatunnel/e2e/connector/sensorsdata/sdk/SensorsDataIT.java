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

package org.apache.seatunnel.e2e.connector.sensorsdata.sdk;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SensorsDataIT extends TestSuiteBase implements TestResource {

    @BeforeAll
    @Override
    public void startUp() throws Exception {}

    @AfterAll
    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason =
                    "spark involves the old version of jackson(2.4.0 involved, but 2.12.x is required) will cause an serialize error.")
    public void testEvents(TestContainer container) throws Exception {
        String jobConfig = "/fake_to_sensorsdata_events.conf";
        Container.ExecResult execResult = container.executeJob(jobConfig);
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason =
                    "spark involves the old version of jackson(2.4.0 involved, but 2.12.x is required) will cause an serialize error.")
    public void testUsers(TestContainer container) throws Exception {
        String jobConfig = "/fake_to_sensorsdata_users.conf";
        Container.ExecResult execResult = container.executeJob(jobConfig);
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason =
                    "spark involves the old version of jackson(2.4.0 involved, but 2.12.x is required) will cause an serialize error.")
    public void testDetails(TestContainer container) throws Exception {
        String jobConfig = "/fake_to_sensorsdata_details.conf";
        Container.ExecResult execResult = container.executeJob(jobConfig);
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
