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

package org.apache.seatunnel.e2e.connector.file.adls;

import org.apache.seatunnel.connectors.seatunnel.file.adls.config.ADLSRuntimeCompatibility;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.AbstractTestContainer;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.MavenJarUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Exercises the ADLSFile source and sink over ABFS against real ADLS Gen2. Enable with {@code
 * SEATUNNEL_ADLS_IT=true}, {@code SEATUNNEL_ADLS_ACCOUNT}, {@code SEATUNNEL_ADLS_CONTAINER}, {@code
 * SEATUNNEL_ADLS_ACCOUNT_KEY}, and an absolute container-relative {@code
 * SEATUNNEL_ADLS_TEST_PREFIX}. The test forwards these values to each engine container before it
 * starts so the job HOCON can resolve its environment references. Hadoop 2.7 engine images are
 * excluded because this connector requires the Hadoop 3 Azure runtime.
 */
@EnabledIfEnvironmentVariable(named = "SEATUNNEL_ADLS_IT", matches = "(?i)true")
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {EngineType.FLINK},
        disabledReason =
                "The Azure Hadoop filesystem runtime requires Hadoop 3, but these images use Hadoop 2.7")
public class ADLSFileIT extends TestSuiteBase implements TestResource {

    private static final String ACCOUNT_ENV = "SEATUNNEL_ADLS_ACCOUNT";
    private static final String CONTAINER_ENV = "SEATUNNEL_ADLS_CONTAINER";
    private static final String ACCOUNT_KEY_ENV = "SEATUNNEL_ADLS_ACCOUNT_KEY";
    private static final String TEST_PREFIX_ENV = "SEATUNNEL_ADLS_TEST_PREFIX";
    private static final String TEST_ROOT_ENV = "SEATUNNEL_ADLS_TEST_ROOT";
    private static final String ADLS_PLUGIN_DIRECTORY =
            "/tmp/seatunnel/plugins/connector-file-adls";
    private static final String ADLS_RUNTIME_JAR = "connector-file-adls-runtime.jar";
    private static final String ADLS_WRITE_JOB = "/adls/adls_file_to_file.conf";
    private static final String ADLS_READ_JOB = "/adls/adls_file_to_assert.conf";

    private FileSystem fileSystem;
    private String testRoot;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.staged(ADLS_RUNTIME_JAR).copyTo(container, ADLS_PLUGIN_DIRECTORY);
                DependencyJar.staged(MavenJarUtil.getHadoop3UberJarName())
                        .copyTo(container, ADLS_PLUGIN_DIRECTORY);
                verifyContainerEnvironment(container);
            };

    public ADLSFileIT() {
        containersFactory =
                () -> {
                    Map<String, String> environment = new HashMap<>();
                    // Exercise the connector's normalization of substituted environment values.
                    environment.put(ACCOUNT_ENV, requiredEnvironment(ACCOUNT_ENV).trim() + " ");
                    environment.put(CONTAINER_ENV, requiredEnvironment(CONTAINER_ENV).trim() + " ");
                    environment.put(
                            ACCOUNT_KEY_ENV, requiredEnvironment(ACCOUNT_KEY_ENV).trim() + " ");
                    environment.put(TEST_ROOT_ENV, testRoot(requiredEnvironment(TEST_PREFIX_ENV)));
                    List<TestContainer> containers = ContainerUtil.discoverTestContainers();
                    containers.forEach(
                            container ->
                                    ((AbstractTestContainer) container)
                                            .setEnvironmentVariables(environment));
                    return containers;
                };
    }

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        String account = requiredEnvironment(ACCOUNT_ENV).trim();
        String storageContainer = requiredEnvironment(CONTAINER_ENV).trim();
        String accountKey = requiredEnvironment(ACCOUNT_KEY_ENV).trim();
        testRoot = testRoot(requiredEnvironment(TEST_PREFIX_ENV));

        Configuration configuration =
                ADLSRuntimeCompatibility.newConfiguration(account, storageContainer);
        ADLSRuntimeCompatibility.configureSharedKey(configuration, account, accountKey);
        fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(testRoot), true);

        writeTestFile("input/orders.csv", "id,name\n1,order-a\n2,order-b\n");
        writeTestFile("input/customers.csv", "id,name\n3,customer-a\n4,customer-b\n");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (fileSystem != null) {
            try {
                if (testRoot != null) {
                    fileSystem.delete(new Path(testRoot), true);
                }
            } finally {
                fileSystem.close();
            }
        }
    }

    @TestTemplate
    public void testFileRoundTrip(TestContainer container) throws Exception {
        SeaTunnelContainer seaTunnelContainer =
                container instanceof SeaTunnelContainer ? (SeaTunnelContainer) container : null;
        if (seaTunnelContainer != null) {
            seaTunnelContainer.setAbfsThreadExemptionEnabled(true);
        }
        try {
            runFileRoundTrip(container);
        } finally {
            if (seaTunnelContainer != null) {
                seaTunnelContainer.setAbfsThreadExemptionEnabled(false);
            }
        }
    }

    private void runFileRoundTrip(TestContainer container) throws Exception {
        String engine = container.identifier().name();
        String runId = engine.toLowerCase(Locale.ROOT).replace('_', '-');
        List<String> variables = Arrays.asList("RUN_ID=" + runId);
        String outputDirectory = "output/" + runId;
        String temporaryDirectory = "tmp/" + runId;
        String staleFile = outputDirectory + "/stale.csv";

        // DROP_DATA must remove this object before the sink commits its new output.
        writeTestFile(staleFile, "id,name\n99,stale\n");

        Container.ExecResult writeResult = container.executeJob(ADLS_WRITE_JOB, variables);
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());

        Assertions.assertTrue(
                containsFiles(outputDirectory), "ADLS output was not created for " + engine);
        Assertions.assertFalse(
                exists(staleFile), "DROP_DATA did not remove the stale object for " + engine);
        Assertions.assertFalse(
                containsFiles(temporaryDirectory),
                "The sink left uncommitted files in its temporary path for " + engine);

        Container.ExecResult readResult = container.executeJob(ADLS_READ_JOB, variables);
        Assertions.assertEquals(
                0,
                readResult.getExitCode(),
                "ADLS read-back failed for " + engine + ": " + readResult.getStderr());
    }

    private void writeTestFile(String relativePath, String contents) throws IOException {
        Path path = new Path(testRoot, relativePath);
        fileSystem.mkdirs(path.getParent());
        try (FSDataOutputStream output = fileSystem.create(path, true)) {
            output.write(contents.getBytes(StandardCharsets.UTF_8));
        }
    }

    private boolean exists(String relativePath) throws IOException {
        return fileSystem.exists(new Path(testRoot, relativePath));
    }

    private boolean containsFiles(String relativePath) throws IOException {
        Path path = new Path(testRoot, relativePath);
        return fileSystem.exists(path) && fileSystem.listFiles(path, true).hasNext();
    }

    private static String testRoot(String prefix) {
        String normalized = prefix.trim().replace('\\', '/');
        while (normalized.length() > 1 && normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        if (!normalized.startsWith("/")) {
            normalized = "/" + normalized;
        }
        for (String segment : normalized.split("/")) {
            if (".".equals(segment) || "..".equals(segment)) {
                throw new IllegalArgumentException(
                        TEST_PREFIX_ENV + " must not contain path traversal segments");
            }
        }
        String base = "/".equals(normalized) ? "" : normalized;
        return base + "/file-round-trip-e2e";
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(name + " must be set when SEATUNNEL_ADLS_IT=true");
        }
        return value;
    }

    private static void verifyContainerEnvironment(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.execInContainer(
                        "sh",
                        "-c",
                        "test -n \"$SEATUNNEL_ADLS_ACCOUNT\""
                                + " && test -n \"$SEATUNNEL_ADLS_CONTAINER\""
                                + " && test -n \"$SEATUNNEL_ADLS_ACCOUNT_KEY\""
                                + " && test -n \"$SEATUNNEL_ADLS_TEST_ROOT\"");
        Assertions.assertEquals(
                0,
                result.getExitCode(),
                "ADLS environment variables are missing from engine container "
                        + container.getDockerImageName());
    }
}
