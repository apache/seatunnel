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
import org.apache.seatunnel.e2e.common.util.PythonTestImage;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import com.github.dockerjava.api.command.InspectImageResponse;

import java.io.IOException;

/**
 * End-to-end coverage for the Python source connector on the SeaTunnel Zeta container.
 *
 * <p>This test exercises the documented stdin JSON plus stdout text contract with the real
 * connector packaging and plugin discovery path.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PythonIT extends SeaTunnelContainer {

    private static final String PYTHON_EXECUTABLE_ALLOWLIST =
            "/usr/bin/python3,/usr/local/bin/python3,/opt/bitnami/python/bin/python3,"
                    + "/usr/bin/python,/usr/local/bin/python";

    @Override
    protected String getDockerImage() {
        return PythonTestImage.withPython(super.getDockerImage());
    }

    @Override
    protected String[] buildStartCommand() {
        return new String[] {
            "env",
            "JAVA_TOOL_OPTIONS=-Dseatunnel.source.python.enabled=true"
                    + " -Dseatunnel.source.python.allowed-executables="
                    + PYTHON_EXECUTABLE_ALLOWLIST,
            super.buildStartCommand()[0]
        };
    }

    /** Starts the SeaTunnel container and copies the Python script used by the e2e job. */
    @BeforeAll
    public void startUp() throws Exception {
        super.startUp();
        Container.ExecResult execResult = server.execInContainer("python3", "--version");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
        copyFileToContainer("/python/emit_rows.py", "/tmp/emit_rows.py");
    }

    /** Stops the SeaTunnel container after the connector e2e job finishes. */
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Verifies exact output on successive jobs in the same engine after the previous Python reader
     * has closed.
     */
    @RepeatedTest(2)
    public void testPythonSourceToAssert() throws IOException, InterruptedException {
        Container.ExecResult result = executeJob("/python_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    /** Verifies image preparation preserves the engine and needs no runtime mirror access. */
    @Test
    public void testPreparedImageWithoutRuntimeNetwork() throws Exception {
        String sourceImage = super.getDockerImage();
        String preparedImage = PythonTestImage.withPython(sourceImage);
        InspectImageResponse source =
                DockerClientFactory.lazyClient().inspectImageCmd(sourceImage).exec();
        InspectImageResponse prepared =
                DockerClientFactory.lazyClient().inspectImageCmd(preparedImage).exec();

        Assertions.assertEquals(source.getArch(), prepared.getArch());
        Assertions.assertArrayEquals(
                source.getConfig().getEntrypoint(), prepared.getConfig().getEntrypoint());
        Assertions.assertArrayEquals(source.getConfig().getEnv(), prepared.getConfig().getEnv());
        Assertions.assertEquals(
                source.getConfig().getWorkingDir(), prepared.getConfig().getWorkingDir());
        Assertions.assertEquals(source.getConfig().getUser(), prepared.getConfig().getUser());

        try (GenericContainer<?> baseline =
                        new GenericContainer<>(sourceImage)
                                .withNetworkMode("none")
                                .withCommand("sleep", "infinity");
                GenericContainer<?> runtime =
                        new GenericContainer<>(preparedImage)
                                .withNetworkMode("none")
                                .withCommand("sleep", "infinity")) {
            baseline.start();
            runtime.start();
            Container.ExecResult python =
                    runtime.execInContainer(
                            "python3",
                            "-c",
                            "import json,sys; print(json.dumps([1, 'ok'])); sys.exit(0)");
            Assertions.assertEquals(0, python.getExitCode(), python.getStderr());
            Assertions.assertEquals("[1, \"ok\"]", python.getStdout().trim());
            Container.ExecResult java = runtime.execInContainer("java", "-version");
            Assertions.assertEquals(0, java.getExitCode(), java.getStderr());
            Container.ExecResult baselineJava = baseline.execInContainer("java", "-version");
            Assertions.assertEquals(0, baselineJava.getExitCode(), baselineJava.getStderr());
            Assertions.assertEquals(baselineJava.getStderr(), java.getStderr());
            Assertions.assertEquals(
                    0,
                    runtime.execInContainer(
                                    "sh",
                                    "-c",
                                    "command -v jps && command -v jstack && command -v jmap")
                            .getExitCode());
        }
    }
}
