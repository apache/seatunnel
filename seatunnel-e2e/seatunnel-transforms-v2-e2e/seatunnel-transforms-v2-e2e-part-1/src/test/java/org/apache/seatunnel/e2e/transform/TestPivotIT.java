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

/**
 * E2E tests for Pivot transform.
 *
 * <p>This tests the following scenarios:
 *
 * <ul>
 *   <li>Basic pivot with single group by key
 *   <li>Pivot with multiple group by keys
 * </ul>
 */
public class TestPivotIT extends TestSuiteBase {

    /**
     * Test basic pivot transform with single group by key.
     *
     * <p>Input: | id | type | value | |----|------|-------| | 1 | A | 100 | | 1 | B | 200 | | 2 | A
     * | 150 | | 2 | C | 300 |
     *
     * <p>Expected Output: | id | A | B | C | |----|-----|------|------| | 1 | 100 | 200 | null | |
     * 2 | 150 | null | 300 |
     */
    @TestTemplate
    public void testPivotTransform(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/pivot_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    /**
     * Test pivot transform with multiple group by keys.
     *
     * <p>Groups by store_id and date, pivots on metric column.
     */
    @TestTemplate
    public void testPivotTransformMultiKeys(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/pivot_transform_multi_keys.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
