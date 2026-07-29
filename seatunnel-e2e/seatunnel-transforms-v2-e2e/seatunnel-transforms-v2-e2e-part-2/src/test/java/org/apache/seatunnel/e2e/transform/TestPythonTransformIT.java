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
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestContainersFactory;
import org.apache.seatunnel.e2e.common.container.flink.Flink13Container;
import org.apache.seatunnel.e2e.common.container.flink.Flink14Container;
import org.apache.seatunnel.e2e.common.container.flink.Flink15Container;
import org.apache.seatunnel.e2e.common.container.flink.Flink16Container;
import org.apache.seatunnel.e2e.common.container.flink.Flink17Container;
import org.apache.seatunnel.e2e.common.container.flink.Flink18Container;
import org.apache.seatunnel.e2e.common.container.flink.Flink20Container;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.ContainerTestingExtension;
import org.apache.seatunnel.e2e.common.junit.TestCaseInvocationContextProvider;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.junit.TestContainers;
import org.apache.seatunnel.e2e.common.junit.TestLoggerExtension;
import org.apache.seatunnel.e2e.common.junit.TimingExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** Verifies the Python transform in the shared E2E container harness. */
@ExtendWith({
    ContainerTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class,
    TimingExtension.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestPythonTransformIT {

    private static final String BASE_PATH = "/python_transform/";
    private static final String SEATUNNEL_CONFIG_DIR = "/tmp/seatunnel/config";
    private static final String PYTHON_EXECUTABLE_ALLOWLIST =
            "/usr/bin/python3,/usr/local/bin/python3,/opt/bitnami/python/bin/python3,"
                    + "/usr/bin/python,/usr/local/bin/python";
    private static final String PYTHON_TRANSFORM_ENABLED_PROPERTY =
            "seatunnel.transform.python.enabled";
    private static final String PYTHON_ALLOWED_EXECUTABLES_PROPERTY =
            "seatunnel.transform.python.allowed-executables";
    private static final String PYTHON_POLICY_JAVA_OPTS =
            "-D"
                    + PYTHON_TRANSFORM_ENABLED_PROPERTY
                    + "=true -D"
                    + PYTHON_ALLOWED_EXECUTABLES_PROPERTY
                    + "="
                    + PYTHON_EXECUTABLE_ALLOWLIST;

    @TestContainers
    private final TestContainersFactory containersFactory =
            () ->
                    ContainerUtil.discoverTestContainers().stream()
                            .map(TestPythonTransformIT::withPythonTransformPolicy)
                            .collect(Collectors.toList());

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                // PythonTransform launches a local worker process inside each runtime container,
                // so the E2E suite must ensure python3 and path-based scripts are present
                // everywhere before the job starts.
                Container.ExecResult execResult = installPythonAndConfigurePolicy(container);
                Assertions.assertEquals(
                        0,
                        execResult.getExitCode(),
                        execResult.getStdout() + System.lineSeparator() + execResult.getStderr());
                ContainerUtil.copyFileIntoContainers(
                        BASE_PATH + "python_transform_path.py",
                        "/tmp/python_transform_path.py",
                        container);
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
        Container.ExecResult execResult =
                container.executeJob(BASE_PATH + "python_transform_path.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    /**
     * Replaces the default container providers with variants that configure Python policy before
     * each runtime JVM starts.
     *
     * @param container discovered default container
     * @return container equivalent with Python transform JVM policy
     */
    private static TestContainer withPythonTransformPolicy(TestContainer container) {
        TestContainerId containerId = container.identifier();
        switch (containerId) {
            case SEATUNNEL:
                return new PythonPolicySeaTunnelContainer();
            case FLINK_1_13:
                return new PythonPolicyFlink13Container();
            case FLINK_1_14:
                return new PythonPolicyFlink14Container();
            case FLINK_1_15:
                return new PythonPolicyFlink15Container();
            case FLINK_1_16:
                return new PythonPolicyFlink16Container();
            case FLINK_1_17:
                return new PythonPolicyFlink17Container();
            case FLINK_1_18:
                return new PythonPolicyFlink18Container();
            case FLINK_1_20:
                return new PythonPolicyFlink20Container();
            case SPARK_2_4:
            case SPARK_3_3:
                return container;
            default:
                throw new IllegalStateException(
                        "Unsupported Python transform E2E container: " + containerId);
        }
    }

    /**
     * Installs python3 if necessary and writes launch-time JVM policy files used by SeaTunnel
     * starters.
     *
     * @param container runtime container
     * @return shell execution result
     * @throws IOException when the shell command cannot run
     * @throws InterruptedException when the shell command is interrupted
     */
    private static Container.ExecResult installPythonAndConfigurePolicy(
            GenericContainer<?> container) throws IOException, InterruptedException {
        return container.execInContainer(
                "bash",
                "-c",
                "set -e;"
                        + " if command -v python3 >/dev/null 2>&1; then"
                        + "   python3 --version;"
                        + " elif command -v apt-get >/dev/null 2>&1; then"
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
                        + " if [ -d "
                        + SEATUNNEL_CONFIG_DIR
                        + " ]; then"
                        + "   grep -q '"
                        + PYTHON_TRANSFORM_ENABLED_PROPERTY
                        + "' "
                        + SEATUNNEL_CONFIG_DIR
                        + "/seatunnel-env.sh 2>/dev/null ||"
                        + "   printf 'JAVA_OPTS=\"${JAVA_OPTS} %s\"\\n' '"
                        + PYTHON_POLICY_JAVA_OPTS
                        + "' >> "
                        + SEATUNNEL_CONFIG_DIR
                        + "/seatunnel-env.sh;"
                        + "   for file in "
                        + SEATUNNEL_CONFIG_DIR
                        + "/jvm_master_options "
                        + SEATUNNEL_CONFIG_DIR
                        + "/jvm_worker_options "
                        + SEATUNNEL_CONFIG_DIR
                        + "/jvm_server_options "
                        + SEATUNNEL_CONFIG_DIR
                        + "/jvm_client_options; do"
                        + "     if [ -f \"$file\" ] && ! grep -q '"
                        + PYTHON_TRANSFORM_ENABLED_PROPERTY
                        + "' \"$file\"; then"
                        + "       printf '%s\\n' '-D"
                        + PYTHON_TRANSFORM_ENABLED_PROPERTY
                        + "=true' >> \"$file\";"
                        + "       printf '%s\\n' '-D"
                        + PYTHON_ALLOWED_EXECUTABLES_PROPERTY
                        + "="
                        + PYTHON_EXECUTABLE_ALLOWLIST
                        + "' >> \"$file\";"
                        + "     fi;"
                        + "   done;"
                        + " fi;"
                        + " python3 --version;"
                        + " echo 'Python transform policy allowlists "
                        + PYTHON_EXECUTABLE_ALLOWLIST
                        + "'");
    }

    /** Injects Python policy before the SeaTunnel server JVM is launched. */
    private static class PythonPolicySeaTunnelContainer extends SeaTunnelContainer {
        @Override
        protected void executeExtraCommands(GenericContainer<?> container)
                throws IOException, InterruptedException {
            super.executeExtraCommands(container);
            installPythonAndConfigurePolicy(container);
        }
    }

    /** Flink 1.13 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink13Container extends Flink13Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /** Flink 1.14 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink14Container extends Flink14Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /** Flink 1.15 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink15Container extends Flink15Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /** Flink 1.16 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink16Container extends Flink16Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /** Flink 1.17 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink17Container extends Flink17Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /** Flink 1.18 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink18Container extends Flink18Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /** Flink 1.20 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink20Container extends Flink20Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }
    }

    /**
     * Appends the Python transform system properties to the Flink JVM option line.
     *
     * @param properties base Flink properties
     * @return copied properties with Python policy
     */
    private static List<String> withFlinkPolicy(List<String> properties) {
        List<String> updatedProperties = new ArrayList<>(properties);
        for (int index = 0; index < updatedProperties.size(); index++) {
            String property = updatedProperties.get(index);
            if (property.trim().startsWith("env.java.opts:")) {
                updatedProperties.set(index, property + " " + PYTHON_POLICY_JAVA_OPTS);
                return updatedProperties;
            }
        }
        updatedProperties.add("env.java.opts: " + PYTHON_POLICY_JAVA_OPTS);
        return updatedProperties;
    }
}
