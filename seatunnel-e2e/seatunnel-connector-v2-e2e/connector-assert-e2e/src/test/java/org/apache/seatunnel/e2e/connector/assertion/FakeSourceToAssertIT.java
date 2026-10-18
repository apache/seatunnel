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

package org.apache.seatunnel.e2e.connector.assertion;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class FakeSourceToAssertIT extends TestSuiteBase {

    @TestTemplate
    public void testFakeSourceToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fakesource_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testFakeSourceToAssertRowSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fake_row_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Currently FLINK unsupported multi table")
    public void testFakeSourceToMultiAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fakesource_to_multi_table_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK},
            disabledReason = "Currently FLINK engine unsupported NULL type")
    public void testFakeFullTypesToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fake_full_types_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    /** DECIMAL above precision 38 keeps its values and type, also through a transform on Spark. */
    @TestTemplate
    public void testWideDecimalToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fake_wide_decimal_to_assert.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    /** Spark cannot carry DECIMAL above precision 38 inside a MAP and must say so at job start. */
    @TestTemplate
    public void testWideDecimalInMapToAssertSink(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fake_wide_decimal_map_to_assert.conf");
        if (container.identifier().getEngineType() != EngineType.SPARK) {
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            return;
        }
        Assertions.assertNotEquals(0, execResult.getExitCode());
        String output = execResult.getStdout() + execResult.getStderr();
        Assertions.assertTrue(output.contains("SPARK-TRANSLATION-01"), output);
        Assertions.assertTrue(output.contains("`c_attrs.value`"), output);
        Assertions.assertTrue(output.contains("DECIMAL(39, 0)"), output);
    }
}
