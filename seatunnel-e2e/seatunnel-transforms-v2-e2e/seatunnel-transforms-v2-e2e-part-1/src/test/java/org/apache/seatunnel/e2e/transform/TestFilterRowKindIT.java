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

public class TestFilterRowKindIT extends TestSuiteBase {

    @TestTemplate
    public void testFilterRowKind(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult1 =
                container.executeJob("/filter_row_kind_exclude_delete.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
        Container.ExecResult execResult2 =
                container.executeJob("/filter_row_kind_exclude_insert.conf");
        Assertions.assertEquals(0, execResult2.getExitCode());
        Container.ExecResult execResult3 =
                container.executeJob("/filter_row_kind_include_insert.conf");
        Assertions.assertEquals(0, execResult3.getExitCode());

        Container.ExecResult execResult4 =
                container.executeJob("/filter_row_to_next_transform.json");
        Assertions.assertEquals(0, execResult4.getExitCode());
    }

    @TestTemplate
    public void testFilterRowKindMultiTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob("/filter_row_kind_exclude_insert_multi_table.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
