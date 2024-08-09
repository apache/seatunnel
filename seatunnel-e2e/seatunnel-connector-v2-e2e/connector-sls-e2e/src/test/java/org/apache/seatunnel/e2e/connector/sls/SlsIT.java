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

package org.apache.seatunnel.e2e.connector.sls;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
@Disabled("Disabled because it needs user's personal sls account to run this test")
public class SlsIT extends TestSuiteBase implements TestResource {

    @BeforeEach
    @Override
    public void startUp() throws Exception {}

    @AfterEach
    @Override
    public void tearDown() throws Exception {}

    @TestTemplate
    public void testSlsStreamingSource(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult1 =
                container.executeJob("/sls_source_with_schema_to_console.conf");
        Assertions.assertEquals(0, execResult1.getExitCode(), execResult1.getStderr());
        Container.ExecResult execResult2 =
                container.executeJob("/sls_source_without_schema_to_console.conf");
        Assertions.assertEquals(0, execResult2.getExitCode(), execResult2.getStderr());
    }
}
