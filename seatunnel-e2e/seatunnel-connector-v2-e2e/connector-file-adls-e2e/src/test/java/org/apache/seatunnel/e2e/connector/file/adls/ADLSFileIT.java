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
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobContainer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.stream.Stream;

/**
 * Exercises ADLSFile read, write, rename, and delete behavior against Azurite in normal CI.
 *
 * <p>Azurite exposes Azure Blob Storage rather than the ADLS Gen2 DFS API, so the default path uses
 * Hadoop's Azure Blob filesystem beneath the same ADLSFile source and sink. Set {@code
 * SEATUNNEL_ADLS_IT=true} together with {@code SEATUNNEL_ADLS_ACCOUNT}, {@code
 * SEATUNNEL_ADLS_CONTAINER}, {@code SEATUNNEL_ADLS_ACCOUNT_KEY}, and an absolute container-relative
 * {@code SEATUNNEL_ADLS_TEST_PREFIX} to run the same test against real ADLS Gen2 through ABFS.
 */
public class ADLSFileIT extends TestSuiteBase implements TestResource {

    private static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
    private static final String AZURITE_ACCOUNT = "devstoreaccount1";
    private static final String AZURITE_ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final String AZURITE_CONTAINER = "files";
    private static final String AZURITE_ENDPOINT_SUFFIX = "blob.azurite.test";
    private static final String AZURITE_NETWORK_ALIAS =
            AZURITE_ACCOUNT + "." + AZURITE_ENDPOINT_SUFFIX;
    private static final int AZURITE_BLOB_PORT = 80;
    private static final String DEFAULT_ENDPOINT_SUFFIX = "dfs.core.windows.net";
    private static final String CLOUD_TEST_ENV = "SEATUNNEL_ADLS_IT";
    private static final String ACCOUNT_ENV = "SEATUNNEL_ADLS_ACCOUNT";
    private static final String CONTAINER_ENV = "SEATUNNEL_ADLS_CONTAINER";
    private static final String ACCOUNT_KEY_ENV = "SEATUNNEL_ADLS_ACCOUNT_KEY";
    private static final String TEST_PREFIX_ENV = "SEATUNNEL_ADLS_TEST_PREFIX";
    private static final String SECRET_CONFIG_PATH = "/tmp/seatunnel/config/adls-e2e-secrets.conf";
    private static final String ADLS_PLUGIN_DIRECTORY =
            "/tmp/seatunnel/plugins/connector-file-adls";

    private GenericContainer<?> azurite;
    private CloudBlobContainer blobContainer;
    private FileSystem fileSystem;
    private String testRoot;
    private boolean cloudTest;

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
        cloudTest = isCloudTestEnabled();
        if (cloudTest) {
            startCloudTest();
        } else {
            startAzuriteTest();
        }
        writeTestFile("input/orders.csv", "id,name\n1,order-a\n2,order-b\n");
        writeTestFile("input/customers.csv", "id,name\n3,customer-a\n4,customer-b\n");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            if (fileSystem != null) {
                try {
                    if (testRoot != null) {
                        fileSystem.delete(new Path(testRoot), true);
                    }
                } finally {
                    fileSystem.close();
                }
            } else if (blobContainer != null) {
                blobContainer.deleteIfExists();
            }
        } finally {
            if (azurite != null) {
                azurite.close();
            }
        }
    }

    @TestTemplate
    public void testFileRoundTrip(TestContainer container) throws Exception {
        String runId = container.identifier().name().toLowerCase(Locale.ROOT).replace('_', '-');
        List<String> variables = Arrays.asList("RUN_ID=" + runId);
        String outputDirectory = "output/" + runId;
        String temporaryDirectory = "tmp/" + runId;
        String staleFile = outputDirectory + "/stale.csv";

        // DROP_DATA must remove this object before the sink commits its new output.
        writeTestFile(staleFile, "id,name\n99,stale\n");

        Container.ExecResult writeResult =
                container.executeJob("/adls/adls_file_to_file.conf", variables);
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());

        Assertions.assertTrue(containsFiles(outputDirectory), "ADLS output was not created");
        Assertions.assertFalse(exists(staleFile), "DROP_DATA did not remove the stale object");
        Assertions.assertFalse(
                containsFiles(temporaryDirectory),
                "The sink did not rename and clean up its temporary output");

        Container.ExecResult readResult =
                container.executeJob("/adls/adls_file_to_assert.conf", variables);
        Assertions.assertEquals(0, readResult.getExitCode(), readResult.getStderr());
    }

    private void startCloudTest() throws Exception {
        String account = requiredEnvironment(ACCOUNT_ENV);
        String storageContainer = requiredEnvironment(CONTAINER_ENV);
        String accountKey = requiredEnvironment(ACCOUNT_KEY_ENV);
        testRoot = testRoot(requiredEnvironment(TEST_PREFIX_ENV));

        Configuration configuration =
                ADLSRuntimeCompatibility.newConfiguration(account, storageContainer);
        ADLSRuntimeCompatibility.configureSharedKey(configuration, account, accountKey);
        fileSystem = FileSystem.get(configuration);
        fileSystem.delete(new Path(testRoot), true);
    }

    private void startAzuriteTest() throws Exception {
        DockerImageName image = DockerImageName.parse(AZURITE_IMAGE);
        azurite =
                new GenericContainer<>(image)
                        .withCommand(
                                "azurite-blob",
                                "--blobHost",
                                "0.0.0.0",
                                "--blobPort",
                                String.valueOf(AZURITE_BLOB_PORT),
                                "--skipApiVersionCheck",
                                "--loose")
                        .withNetwork(NETWORK)
                        .withNetworkAliases(AZURITE_NETWORK_ALIAS)
                        .withExposedPorts(AZURITE_BLOB_PORT)
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                image.asCanonicalNameString())));
        Startables.deepStart(Stream.of(azurite)).join();

        blobContainer =
                CloudStorageAccount.parse(hostConnectionString())
                        .createCloudBlobClient()
                        .getContainerReference(AZURITE_CONTAINER);
        blobContainer.createIfNotExists();
        testRoot = "/file-round-trip-e2e";
    }

    private void writeTestFile(String relativePath, String contents) throws Exception {
        if (cloudTest) {
            writeCloudFile(new Path(testRoot, relativePath), contents);
        } else {
            blobContainer.getBlockBlobReference(blobName(relativePath)).uploadText(contents);
        }
    }

    private void writeCloudFile(Path path, String contents) throws IOException {
        fileSystem.mkdirs(path.getParent());
        try (FSDataOutputStream output = fileSystem.create(path, true)) {
            output.write(contents.getBytes(StandardCharsets.UTF_8));
        }
    }

    private boolean exists(String relativePath) throws Exception {
        if (cloudTest) {
            return fileSystem.exists(new Path(testRoot, relativePath));
        }
        return blobContainer.getBlockBlobReference(blobName(relativePath)).exists();
    }

    private boolean containsFiles(String relativePath) throws Exception {
        if (cloudTest) {
            return fileSystem.exists(new Path(testRoot, relativePath));
        }
        return blobContainer.listBlobs(blobName(relativePath) + "/", true).iterator().hasNext();
    }

    private String blobName(String relativePath) {
        String root = testRoot.startsWith("/") ? testRoot.substring(1) : testRoot;
        return root + "/" + relativePath;
    }

    private String hostConnectionString() {
        return "DefaultEndpointsProtocol=http;AccountName="
                + AZURITE_ACCOUNT
                + ";AccountKey="
                + AZURITE_ACCOUNT_KEY
                + ";BlobEndpoint=http://"
                + azurite.getHost()
                + ":"
                + azurite.getMappedPort(AZURITE_BLOB_PORT)
                + "/"
                + AZURITE_ACCOUNT
                + ";";
    }

    private static void copySecretConfiguration(GenericContainer<?> container)
            throws IOException, InterruptedException {
        boolean cloudTest = isCloudTestEnabled();
        String account = cloudTest ? requiredEnvironment(ACCOUNT_ENV) : AZURITE_ACCOUNT;
        String storageContainer =
                cloudTest ? requiredEnvironment(CONTAINER_ENV) : AZURITE_CONTAINER;
        String accountKey = cloudTest ? requiredEnvironment(ACCOUNT_KEY_ENV) : AZURITE_ACCOUNT_KEY;
        String endpointSuffix = cloudTest ? DEFAULT_ENDPOINT_SUFFIX : AZURITE_ENDPOINT_SUFFIX;
        String root =
                cloudTest ? testRoot(requiredEnvironment(TEST_PREFIX_ENV)) : "/file-round-trip-e2e";
        String hadoopProperties = cloudTest ? "{}" : "{ \"fs.azure.test.emulator\" = \"true\" }";
        String contents =
                "adls_e2e {\n"
                        + "  account_name = "
                        + hoconString(account)
                        + "\n  container = "
                        + hoconString(storageContainer)
                        + "\n  account_key = "
                        + hoconString(accountKey)
                        + "\n  endpoint_suffix = "
                        + hoconString(endpointSuffix)
                        + "\n  test_root = "
                        + hoconString(root)
                        + "\n  hadoop_properties = "
                        + hadoopProperties
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
        return base + "/file-round-trip-e2e";
    }

    private static String requiredEnvironment(String name) {
        String value = System.getenv(name);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalStateException(name + " must be set when SEATUNNEL_ADLS_IT=true");
        }
        return value;
    }

    private static boolean isCloudTestEnabled() {
        return Boolean.parseBoolean(System.getenv(CLOUD_TEST_ENV));
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
