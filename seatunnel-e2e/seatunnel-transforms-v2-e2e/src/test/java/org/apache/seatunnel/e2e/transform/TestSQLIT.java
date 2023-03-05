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
import org.apache.seatunnel.e2e.common.container.spark.AbstractTestSparkContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestSQLIT extends TestSuiteBase {

    @TestTemplate
    public void testSQL(TestContainer container) throws IOException, InterruptedException {
        if (container instanceof AbstractTestSparkContainer) {
            return; // skip test for spark, because some problems of Timestamp convert unresolved.
        }

        Container.ExecResult execResult = container.executeJob("/sql_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/sql_transform/binary_expression.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/sql_transform/func_string.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/sql_transform/func_numeric.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/sql_transform/func_datetime.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/sql_transform/func_system.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/sql_transform/criteria_filter.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
