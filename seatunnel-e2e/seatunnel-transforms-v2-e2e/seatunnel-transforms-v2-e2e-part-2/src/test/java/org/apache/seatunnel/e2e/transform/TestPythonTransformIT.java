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
import org.apache.seatunnel.e2e.common.container.flink.AbstractTestFlinkContainer;
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
    private static final String PYTHON_EXECUTABLE_ALLOWLIST =
            "/usr/bin/python3,/usr/local/bin/python3,/opt/bitnami/python/bin/python3,"
                    + "/usr/bin/python,/usr/local/bin/python";
    private static final String PYTHON_TRANSFORM_ENABLED_PROPERTY =
            "seatunnel.transform.python.enabled";
    private static final String PYTHON_ALLOWED_EXECUTABLES_PROPERTY =
            "seatunnel.transform.python.allowed-executables";
    private static final String PYTHON_TRANSFORM_ENABLED_JAVA_OPTION =
            "-D" + PYTHON_TRANSFORM_ENABLED_PROPERTY + "=true";
    private static final String PYTHON_ALLOWED_EXECUTABLES_JAVA_OPTION =
            "-D" + PYTHON_ALLOWED_EXECUTABLES_PROPERTY + "=" + PYTHON_EXECUTABLE_ALLOWLIST;
    private static final String PYTHON_POLICY_JAVA_OPTS =
            PYTHON_TRANSFORM_ENABLED_JAVA_OPTION + " " + PYTHON_ALLOWED_EXECUTABLES_JAVA_OPTION;

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
                Container.ExecResult execResult = installPythonIfNecessary(container);
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
        assertFlinkPythonPolicyVisible(container);
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
        assertFlinkPythonPolicyVisible(container);
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
     * Installs python3 if necessary before Python transform jobs are submitted in the shared E2E
     * harness.
     *
     * @param container runtime container
     * @return shell execution result
     * @throws IOException when the shell command cannot run
     * @throws InterruptedException when the shell command is interrupted
     */
    private static Container.ExecResult installPythonIfNecessary(GenericContainer<?> container)
            throws IOException, InterruptedException {
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
                        + " python3 --version;"
                        + " echo 'Python transform policy allowlists "
                        + PYTHON_EXECUTABLE_ALLOWLIST
                        + "'");
    }

    /** Injects Python policy before the SeaTunnel server JVM is launched. */
    private static class PythonPolicySeaTunnelContainer extends SeaTunnelContainer {
        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.13 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink13Container extends Flink13Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.14 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink14Container extends Flink14Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.15 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink15Container extends Flink15Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.16 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink16Container extends Flink16Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.17 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink17Container extends Flink17Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.18 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink18Container extends Flink18Container {
        @Override
        protected List<String> getFlinkProperties() {
            return withFlinkPolicy(super.getFlinkProperties());
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /** Flink 1.20 container with Python transform JVM policy enabled before startup. */
    private static class PythonPolicyFlink20Container extends Flink20Container {
        @Override
        protected List<String> getFlinkProperties() {
            // Flink 1.20 reads env.java.opts from YAML and collapses duplicated space-delimited
            // -D entries into a single malformed value. Keep the Python policy only in the
            // standard launcher hook so the runtime sees one clean copy of each property.
            return super.getFlinkProperties();
        }

        @Override
        protected String getJavaToolOptions() {
            return PYTHON_POLICY_JAVA_OPTS;
        }
    }

    /**
     * Appends the Python transform system properties to Flink JVM option keys used by different
     * Flink versions.
     *
     * @param properties base Flink properties
     * @return copied properties with Python policy
     */
    private static List<String> withFlinkPolicy(List<String> properties) {
        List<String> updatedProperties = new ArrayList<>(properties);
        appendFlinkJvmPolicy(updatedProperties, "env.java.opts:");
        appendFlinkJvmPolicy(updatedProperties, "env.java.opts.all:");
        return updatedProperties;
    }

    /**
     * Appends the Python policy to an existing Flink option or inserts the option inside the Flink
     * 2.0 replacement section.
     *
     * @param properties copied Flink properties to update
     * @param optionPrefix Flink JVM option key prefix
     */
    private static void appendFlinkJvmPolicy(List<String> properties, String optionPrefix) {
        for (int index = 0; index < properties.size(); index++) {
            String property = properties.get(index);
            if (property.trim().startsWith(optionPrefix)) {
                if (!property.contains(PYTHON_TRANSFORM_ENABLED_PROPERTY)) {
                    properties.set(index, property + " " + PYTHON_POLICY_JAVA_OPTS);
                }
                return;
            }
        }
        properties.add(
                flinkPropertyInsertIndex(properties), optionPrefix + " " + PYTHON_POLICY_JAVA_OPTS);
    }

    /**
     * Finds the insertion point that keeps added properties inside the Flink 2.0 config replacement
     * block when that block is present.
     *
     * @param properties copied Flink properties to inspect
     * @return insertion index for a new Flink property
     */
    private static int flinkPropertyInsertIndex(List<String> properties) {
        for (int index = 0; index < properties.size(); index++) {
            if ("# SEATUNNEL_FLINK20_CONFIG_REPLACE_END".equals(properties.get(index))) {
                return index;
            }
        }
        return properties.size();
    }

    /**
     * Verifies that Flink JobManager/client and TaskManager containers both export the JVM policy
     * hook needed by the already-started runtime processes.
     *
     * @param container runtime selected by the shared E2E harness
     * @throws IOException when docker exec cannot inspect container state
     * @throws InterruptedException when docker exec is interrupted
     */
    private static void assertFlinkPythonPolicyVisible(TestContainer container)
            throws IOException, InterruptedException {
        if (!(container instanceof AbstractTestFlinkContainer)) {
            return;
        }
        AbstractTestFlinkContainer flinkContainer = (AbstractTestFlinkContainer) container;
        assertPythonPolicyJavaOptions(
                "Flink JobManager/client",
                flinkContainer.executeJobManagerInnerCommand("printf '%s' \"$JAVA_TOOL_OPTIONS\""));
        assertPythonPolicyJavaOptions(
                "Flink TaskManager",
                flinkContainer.executeTaskManagerInnerCommand(
                        "printf '%s' \"$JAVA_TOOL_OPTIONS\""));
    }

    /**
     * Ensures the standard JVM launcher hook carries both security properties needed by the Python
     * transform runtime.
     *
     * @param runtimeName human-readable runtime side for assertion messages
     * @param javaToolOptions raw JAVA_TOOL_OPTIONS value observed inside the container
     */
    private static void assertPythonPolicyJavaOptions(String runtimeName, String javaToolOptions) {
        Assertions.assertTrue(
                javaToolOptions.contains(PYTHON_TRANSFORM_ENABLED_JAVA_OPTION),
                runtimeName
                        + " is missing "
                        + PYTHON_TRANSFORM_ENABLED_JAVA_OPTION
                        + " in JAVA_TOOL_OPTIONS: "
                        + javaToolOptions);
        Assertions.assertTrue(
                javaToolOptions.contains(PYTHON_ALLOWED_EXECUTABLES_JAVA_OPTION),
                runtimeName
                        + " is missing "
                        + PYTHON_ALLOWED_EXECUTABLES_JAVA_OPTION
                        + " in JAVA_TOOL_OPTIONS: "
                        + javaToolOptions);
    }
}
