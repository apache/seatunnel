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

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

/** Verifies the Python transform in the shared E2E container harness. */
public class TestPythonTransformIT extends TestSuiteBase {

    private static final String BASE_PATH = "/python_transform/";

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // PythonTransform launches a local worker process inside each runtime container,
                // so the E2E suite must ensure python3 exists before the job starts.
                Container.ExecResult execResult =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "set -e;"
                                        + " if command -v python3 >/dev/null 2>&1; then"
                                        + "   python3 --version;"
                                        + "   exit 0;"
                                        + " fi;"
                                        + " if command -v apt-get >/dev/null 2>&1; then"
                                        + "   apt-get update &&"
                                        + "   DEBIAN_FRONTEND=noninteractive apt-get install -y python3;"
                                        + " elif command -v dnf >/dev/null 2>&1; then"
                                        + "   dnf install -y python3;"
                                        + " elif command -v microdnf >/dev/null 2>&1; then"
                                        + "   microdnf install -y python3;"
                                        + " elif command -v yum >/dev/null 2>&1; then"
                                        + "   yum install -y python3;"
                                        + " elif command -v apk >/dev/null 2>&1; then"
                                        + "   apk add --no-cache python3;"
                                        + " else"
                                        + "   echo 'Unsupported package manager for installing python3' >&2;"
                                        + "   exit 1;"
                                        + " fi;"
                                        + " python3 --version");
                Assertions.assertEquals(
                        0,
                        execResult.getExitCode(),
                        execResult.getStdout() + System.lineSeparator() + execResult.getStderr());
            };

    /**
     * Verifies inline Python source code execution inside the container runtime.
     *
     * @param container test container provided by the shared suite
     * @throws IOException when the job execution API fails
     * @throws InterruptedException when the test is interrupted
     */
    @TestTemplate
    public void testInlinePythonTransform(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult = container.executeJob(BASE_PATH + "python_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    /**
     * Verifies Python scripts can be loaded from a runtime path inside the container.
     *
     * @param container test container provided by the shared suite
     * @throws IOException when the job execution API fails
     * @throws InterruptedException when the test is interrupted
     */
    @TestTemplate
    public void testPathPythonTransform(TestContainer container)
            throws IOException, InterruptedException {
        container.copyFileToContainer(
                BASE_PATH + "python_transform_path.py", "/tmp/python_transform_path.py");
        Container.ExecResult execResult =
                container.executeJob(BASE_PATH + "python_transform_path.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
