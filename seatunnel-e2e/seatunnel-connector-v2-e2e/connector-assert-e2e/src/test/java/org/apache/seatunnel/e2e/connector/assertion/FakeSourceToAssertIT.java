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

    /**
     * The Assert writer declares no replica mode, so a job with {@code multi_table_sink_replica =
     * 2} must fail when its writers are created, with the reason, instead of spreading rows over
     * two writers at random.
     */
    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason =
                    "spark-submit can hang in local mode after a failed job, see"
                            + " apache/seatunnel#13155")
    public void testSinkWithoutReplicaModeIsRejectedWithReplicas(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/assertion/fakesource_to_assert_with_replicas.conf");

        Assertions.assertNotEquals(0, execResult.getExitCode());
        String output = execResult.getStdout() + execResult.getStderr();
        Assertions.assertTrue(
                output.contains("its writer declares neither KEYED nor RANDOM"), output);
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
}
