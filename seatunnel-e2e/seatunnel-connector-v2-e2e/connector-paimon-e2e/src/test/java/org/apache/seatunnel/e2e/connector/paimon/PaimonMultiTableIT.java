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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@DisabledOnContainer(
        value = TestContainerId.FLINK_1_13,
        disabledReason = "Paimon does not support flink 1.13")
public class PaimonMultiTableIT extends TestSuiteBase implements TestResource {

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                container.execInContainer("chmod", "777", "-R", "/tmp/seatunnel_mnt/");
            };

    @TestTemplate
    public void testMultiTableReadAndAssert(TestContainer container)
            throws IOException, InterruptedException {
        // First, write data to multiple tables
        Container.ExecResult writeTable1Result = container.executeJob("/fake_to_paimon_table1.conf");
        Assertions.assertEquals(0, writeTable1Result.getExitCode());
        
        Container.ExecResult writeTable2Result = container.executeJob("/fake_to_paimon_table2.conf");
        Assertions.assertEquals(0, writeTable2Result.getExitCode());
        
        // Then, read from multiple tables using table_list and assert the results
        Container.ExecResult multiTableReadResult = 
                container.executeJob("/paimon-to-assert-with-multitable.conf");
        Assertions.assertEquals(0, multiTableReadResult.getExitCode());
    }

    @TestTemplate
    public void testMultiTableSink(TestContainer container)
            throws IOException, InterruptedException {
        // Test multi-table sink functionality
        Container.ExecResult multiTableSinkResult = 
                container.executeJob("/fake_to_paimon_multi_table.conf");
        Assertions.assertEquals(0, multiTableSinkResult.getExitCode());
    }

    @Override
    public void startUp() throws Exception {}

    @Override
    @AfterEach
    public void tearDown() throws Exception {}
}
