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

package org.apache.seatunnel.e2e.connector.python;

import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;

import java.io.IOException;

/**
 * End-to-end coverage for the Python source connector on the SeaTunnel Zeta container.
 *
 * <p>This test exercises the documented stdin JSON plus stdout text contract with the real
 * connector packaging and plugin discovery path.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PythonIT extends SeaTunnelContainer {

    @Override
    protected String[] buildStartCommand() {
        return new String[] {
            "env",
            "JAVA_TOOL_OPTIONS=-Dseatunnel.source.python.enabled=true -Dseatunnel.source.python.allowed-executables=/usr/bin/python3",
            super.buildStartCommand()[0]
        };
    }

    /** Starts the SeaTunnel container and copies the Python script used by the e2e job. */
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        copyFileToContainer("/python/emit_rows.py", "/tmp/emit_rows.py");
    }

    /** Stops the SeaTunnel container after the connector e2e job finishes. */
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verifies the SeaTunnel Zeta engine can read rows from the documented Python script contract.
     */
    @Test
    public void testPythonSourceToAssert() throws IOException, InterruptedException {
        Container.ExecResult result = executeJob("/python_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
}
