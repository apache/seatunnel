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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class FakeIT extends TestSuiteBase {
    @TestTemplate
    public void testFakeConnector(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult textWriteResult = container.executeJob("/fake_to_assert.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult fakeWithRange =
                container.executeJob("/fake_to_assert_with_range.conf");
        Assertions.assertEquals(0, fakeWithRange.getExitCode());
        Container.ExecResult fakeWithTemplate =
                container.executeJob("/fake_to_assert_with_template.conf");
        Assertions.assertEquals(0, fakeWithTemplate.getExitCode());
        Container.ExecResult fakeComplex =
                container.executeJob("/fake_generic_row_type_to_assert.conf");
        Assertions.assertEquals(0, fakeWithTemplate.getExitCode());
        Container.ExecResult compatibleTableNameCase =
                container.executeJob(
                        "/fake_to_assert_with_compatible_source_and_result_table_name.conf");
        Assertions.assertEquals(0, compatibleTableNameCase.getExitCode());
    }
}
