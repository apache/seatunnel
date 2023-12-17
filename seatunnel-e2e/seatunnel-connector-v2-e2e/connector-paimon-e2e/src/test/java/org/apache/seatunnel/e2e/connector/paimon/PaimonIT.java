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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;

@DisabledOnContainer(
        value = TestContainerId.FLINK_1_13,
        disabledReason = "Paimon does not support flink 1.13")
public class PaimonIT extends TestSuiteBase {

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Path schemaPath = ContainerUtil.getResourcesFile("/schema-0.json").toPath();
                container.copyFileToContainer(
                        MountableFile.forHostPath(schemaPath),
                        "/tmp/paimon/default.db/st_test/schema/schema-0");
                container.execInContainer("chmod", "777", "-R", "/tmp/paimon");
            };

    @TestTemplate
    public void testWriteAndReadPaimon(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_paimon.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult readResult = container.executeJob("/paimon_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }
}
