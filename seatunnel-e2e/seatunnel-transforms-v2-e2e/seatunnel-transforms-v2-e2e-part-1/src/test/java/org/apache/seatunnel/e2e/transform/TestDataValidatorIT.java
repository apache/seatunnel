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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestDataValidatorIT extends TestSuiteBase {

    @TestTemplate
    public void testDataValidatorWithValidData(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/data_validator_valid.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDataValidatorWithSkipMode(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/data_validator_skip.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDataValidatorWithFailMode(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/data_validator_fail.conf");
        // Should fail due to validation errors
        Assertions.assertNotEquals(0, execResult.getExitCode());

        // Check for validation error messages in stderr
        String stderr = execResult.getStderr();
        Assertions.assertNotNull(stderr, "stderr should not be null");
        Assertions.assertTrue(
                stderr.contains("Validation failed") || stderr.contains("VALIDATION_FAILED"),
                "stderr should contain validation error message, but was: " + stderr);

        // Check for specific validation rule failure (NOT_NULL for name field)
        Assertions.assertTrue(
                stderr.contains("name") || stderr.contains("NOT_NULL") || stderr.contains("null"),
                "stderr should contain reference to name field validation failure, but was: "
                        + stderr);
    }

    @TestTemplate
    public void testDataValidatorWithRouteToTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/data_validator_route_to_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDataValidatorWithUDF(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/data_validator_email_udf.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
