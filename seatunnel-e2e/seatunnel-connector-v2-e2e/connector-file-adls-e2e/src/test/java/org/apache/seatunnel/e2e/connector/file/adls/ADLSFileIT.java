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
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;

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
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * Runs against a real HNS-enabled ADLS Gen2 account. Set {@code SEATUNNEL_ADLS_IT=true} together
 * with {@code SEATUNNEL_ADLS_ACCOUNT}, {@code SEATUNNEL_ADLS_CONTAINER}, {@code
 * SEATUNNEL_ADLS_ACCOUNT_KEY}, and an absolute container-relative {@code
 * SEATUNNEL_ADLS_TEST_PREFIX} to enable it.
 */
@EnabledIfEnvironmentVariable(named = "SEATUNNEL_ADLS_IT", matches = "(?i:true)")
public class ADLSFileIT extends TestSuiteBase implements TestResource {

    private static final String ACCOUNT_ENV = "SEATUNNEL_ADLS_ACCOUNT";
    private static final String CONTAINER_ENV = "SEATUNNEL_ADLS_CONTAINER";
    private static final String ACCOUNT_KEY_ENV = "SEATUNNEL_ADLS_ACCOUNT_KEY";
    private static final String TEST_PREFIX_ENV = "SEATUNNEL_ADLS_TEST_PREFIX";
    private static final String SECRET_CONFIG_PATH = "/tmp/seatunnel/config/adls-e2e-secrets.conf";
    private static final String ADLS_PLUGIN_DIRECTORY =
            "/tmp/seatunnel/plugins/connector-file-adls";

    private FileSystem fileSystem;
    private String testRoot;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.of(ADLSRuntimeCompatibility.class)
                        .copyTo(container, ADLS_PLUGIN_DIRECTORY);
                copySecretConfiguration(container);
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        String account = requiredEnvironment(ACCOUNT_ENV);
        String storageContainer = requiredEnvironment(CONTAINER_ENV);
        String accountKey = requiredEnvironment(ACCOUNT_KEY_ENV);
        testRoot = testRoot(requiredEnvironment(TEST_PREFIX_ENV));

        Configuration configuration =
                ADLSRuntimeCompatibility.newConfiguration(account, storageContainer);
        ADLSRuntimeCompatibility.configureSharedKey(configuration, account, accountKey);
        fileSystem = FileSystem.get(configuration);

        Path root = new Path(testRoot);
        fileSystem.delete(root, true);
        writeCsv(new Path(root, "input/orders.csv"), "id,name\n1,order-a\n2,order-b\n");
        writeCsv(new Path(root, "input/customers.csv"), "id,name\n3,customer-a\n4,customer-b\n");
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
    public void testPreserveSourceFilename(TestContainer container) throws Exception {
        String runId = container.identifier().name().toLowerCase(Locale.ROOT).replace('_', '-');
        List<String> writeVariables = Arrays.asList("RUN_ID=" + runId);

        Container.ExecResult writeResult =
                container.executeJob("/adls/adls_preserve_source_filename.conf", writeVariables);
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());

        Path output = new Path(testRoot + "/output/" + runId);
        Assertions.assertTrue(fileSystem.isFile(new Path(output, "orders.csv")));
        Assertions.assertTrue(fileSystem.isFile(new Path(output, "customers.csv")));

        assertPreservedFile(container, runId, "orders.csv", 1, 2);
        assertPreservedFile(container, runId, "customers.csv", 3, 4);
    }

    private void assertPreservedFile(
            TestContainer container, String runId, String fileName, int minId, int maxId)
            throws IOException, InterruptedException {
        List<String> variables =
                Arrays.asList(
                        "RUN_ID=" + runId,
                        "FILE_NAME=" + fileName,
                        "MIN_ID=" + minId,
                        "MAX_ID=" + maxId);
        Container.ExecResult readResult =
                container.executeJob("/adls/adls_preserved_file_to_assert.conf", variables);
        Assertions.assertEquals(0, readResult.getExitCode(), readResult.getStderr());
    }

    private void writeCsv(Path path, String contents) throws IOException {
        fileSystem.mkdirs(path.getParent());
        try (FSDataOutputStream output = fileSystem.create(path, true)) {
            output.write(contents.getBytes(StandardCharsets.UTF_8));
        }
    }

    private static void copySecretConfiguration(GenericContainer<?> container)
            throws IOException, InterruptedException {
        String testRoot = testRoot(requiredEnvironment(TEST_PREFIX_ENV));
        String contents =
                "adls_e2e {\n"
                        + "  account_name = "
                        + hoconString(requiredEnvironment(ACCOUNT_ENV))
                        + "\n  container = "
                        + hoconString(requiredEnvironment(CONTAINER_ENV))
                        + "\n  account_key = "
                        + hoconString(requiredEnvironment(ACCOUNT_KEY_ENV))
                        + "\n  test_root = "
                        + hoconString(testRoot)
                        + "\n}\n";

        java.nio.file.Path secretFile = Files.createTempFile("seatunnel-adls-e2e-", ".conf");
        try {
            Files.write(secretFile, contents.getBytes(StandardCharsets.UTF_8));
            container.copyFileToContainer(
                    MountableFile.forHostPath(secretFile), SECRET_CONFIG_PATH);
            Container.ExecResult chmod =
                    container.execInContainer("chmod", "600", SECRET_CONFIG_PATH);
            Assertions.assertEquals(0, chmod.getExitCode(), chmod.getStderr());
        } finally {
            Files.deleteIfExists(secretFile);
        }
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
        return base + "/preserve-source-filename-e2e";
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(name + " must be set when SEATUNNEL_ADLS_IT=true");
        }
        return value;
    }

    private static String hoconString(String value) {
        StringBuilder result = new StringBuilder(value.length() + 2).append('"');
        for (int index = 0; index < value.length(); index++) {
            char character = value.charAt(index);
            switch (character) {
                case '"':
                    result.append("\\\"");
                    break;
                case '\\':
                    result.append("\\\\");
                    break;
                case '\n':
                    result.append("\\n");
                    break;
                case '\r':
                    result.append("\\r");
                    break;
                case '\t':
                    result.append("\\t");
                    break;
                case '\b':
                    result.append("\\b");
                    break;
                case '\f':
                    result.append("\\f");
                    break;
                default:
                    result.append(character);
            }
        }
        return result.append('"').toString();
    }
}
