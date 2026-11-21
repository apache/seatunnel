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
 * Verifies DefineSinkType through the Assert sink:
 *
 * <ul>
 *   <li>single table mode: the transform level rule declares the sink type
 *   <li>transform level rule plus per-table rules: every table gets its own sink type
 *   <li>per-table rules only: tables without an entry keep their original sink type
 * </ul>
 */
public class TestDefineSinkTypeIT extends TestSuiteBase {

    @TestTemplate
    public void testDefineSinkType(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob("/define_sink_type_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    @TestTemplate
    public void testDefineSinkTypeMultiTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/define_sink_type_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }

    /** A transform configured only through {@code table_transform} entries must be accepted. */
    @TestTemplate
    public void testDefineSinkTypeTableTransformOnly(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/define_sink_type_table_transform_only.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
