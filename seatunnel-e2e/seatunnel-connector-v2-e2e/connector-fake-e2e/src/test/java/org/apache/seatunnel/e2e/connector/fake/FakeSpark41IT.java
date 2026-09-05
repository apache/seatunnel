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
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * End-to-end smoke test that runs a minimal SeaTunnel job on Spark 4.1 through the new starter and
 * translation runtime.
 */
@DisabledOnContainer(
        value = {
            TestContainerId.FLINK_1_13,
            TestContainerId.FLINK_1_14,
            TestContainerId.FLINK_1_15,
            TestContainerId.FLINK_1_16,
            TestContainerId.FLINK_1_17,
            TestContainerId.FLINK_1_18,
            TestContainerId.FLINK_1_20,
            TestContainerId.SPARK_2_4,
            TestContainerId.SPARK_3_3,
            TestContainerId.SEATUNNEL
        },
        disabledReason = "Only runs on Spark 4.1")
@EnabledIfEnvironmentVariable(named = "RUN_SPARK_41_CONTAINER", matches = "true")
public class FakeSpark41IT extends TestSuiteBase {

    @TestTemplate
    public void testFakeToAssertOnSpark41(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result = container.executeJob("/fake_to_assert_spark41.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
