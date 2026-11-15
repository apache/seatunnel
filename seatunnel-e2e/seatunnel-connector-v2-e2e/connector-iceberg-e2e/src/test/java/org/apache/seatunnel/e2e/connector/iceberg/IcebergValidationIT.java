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
package org.apache.seatunnel.e2e.connector.iceberg;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class IcebergValidationIT extends TestSuiteBase {

    @TestTemplate
    public void testSinkUpsertWithoutPrimaryKeysRejected(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/iceberg/iceberg-validation-upsert-without-pk.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String stderr = execResult.getStderr();
        Assertions.assertNotNull(stderr, "stderr should not be null");
        Assertions.assertTrue(
                stderr.contains("Option validation failed") && stderr.contains("primary-keys"),
                "stderr should report the failed primary-keys validation, but was: " + stderr);
    }

    @TestTemplate
    public void testSourceNonPositiveScanIntervalRejected(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/iceberg/iceberg-validation-nonpositive-scan-interval.conf");
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String stderr = execResult.getStderr();
        Assertions.assertNotNull(stderr, "stderr should not be null");
        Assertions.assertTrue(
                stderr.contains("Option validation failed") && stderr.contains("scan-interval"),
                "stderr should report the failed scan-interval validation, but was: " + stderr);
    }
}
