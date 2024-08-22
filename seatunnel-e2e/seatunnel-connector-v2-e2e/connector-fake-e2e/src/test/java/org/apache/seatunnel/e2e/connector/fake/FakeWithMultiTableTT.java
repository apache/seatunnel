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

package org.apache.seatunnel.e2e.connector.fake;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@DisabledOnContainer(
        value = {},
        type = {EngineType.FLINK},
        disabledReason = "Currently SPARK and FLINK do not support multi-table")
public class FakeWithMultiTableTT extends TestSuiteBase {
    @TestTemplate
    public void testFakeConnector(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult fakeWithTableNames =
                container.executeJob("/fake_to_console_with_multitable_mode.conf");
        Assertions.assertFalse(
                container.getServerLogs().contains("MultiTableWriterRunnable error"));
        Assertions.assertEquals(0, fakeWithTableNames.getExitCode());

        Container.ExecResult fakeWithException =
                container.executeJob("/fake_to_assert_with_multitable_exception.conf");
        Assertions.assertTrue(container.getServerLogs().contains("MultiTableWriterRunnable error"));
        Assertions.assertTrue(
                container
                        .getServerLogs()
                        .contains(
                                "at org.apache.seatunnel.connectors.seatunnel.common.multitablesink.MultiTableSinkWriter.checkQueueRemain(MultiTableSinkWriter.java"));
        Assertions.assertEquals(1, fakeWithException.getExitCode());
    }
}
