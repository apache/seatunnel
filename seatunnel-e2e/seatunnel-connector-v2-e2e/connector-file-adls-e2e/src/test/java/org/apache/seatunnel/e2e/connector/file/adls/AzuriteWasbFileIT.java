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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.DependencyJar;
import org.apache.seatunnel.e2e.common.util.MavenJarUtil;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.condition.DisabledIfEnvironmentVariable;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;
import org.testcontainers.utility.MountableFile;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Exercises the generic Hadoop file connector over WASB against Azurite in normal CI. Azurite
 * exposes the Blob API, so this test does not exercise ADLSFile or the Gen2 DFS API.
 */
@DisabledIfEnvironmentVariable(named = "SEATUNNEL_ADLS_IT", matches = "(?i)true")
@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {EngineType.FLINK},
        disabledReason =
                "The Azure Hadoop filesystem runtime requires Hadoop 3, but these images use Hadoop 2.7")
public class AzuriteWasbFileIT extends TestSuiteBase implements TestResource {

    private static final String AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite:3.35.0";
    private static final String AZURITE_ACCOUNT = "devstoreaccount1";
    private static final String AZURITE_ACCOUNT_KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";
    private static final String AZURITE_CONTAINER = "files";
    private static final String AZURITE_ENDPOINT_SUFFIX = "blob.azurite.test";
    private static final String AZURITE_NETWORK_ALIAS =
            AZURITE_ACCOUNT + "." + AZURITE_ENDPOINT_SUFFIX;
    private static final int AZURITE_BLOB_PORT = 80;
    private static final String AZURITE_HADOOP_CONFIG_PATH =
            "/tmp/seatunnel/config/adls-azurite-hadoop.xml";
    private static final String HADOOP_FILE_PLUGIN_DIRECTORY =
            "/tmp/seatunnel/plugins/connector-file-hadoop";
    private static final String ADLS_RUNTIME_JAR = "connector-file-adls-runtime.jar";
    private static final String AZURITE_WRITE_JOB = "/adls/azurite_file_to_file.conf";
    private static final String AZURITE_READ_JOB = "/adls/azurite_file_to_assert.conf";

    private GenericContainer<?> azurite;
    private CloudBlobContainer blobContainer;
    private String testRoot;

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                DependencyJar.staged(ADLS_RUNTIME_JAR)
                        .copyTo(container, HADOOP_FILE_PLUGIN_DIRECTORY);
                DependencyJar.staged(MavenJarUtil.getHadoop3UberJarName())
                        .copyTo(container, HADOOP_FILE_PLUGIN_DIRECTORY);
                copyAzuriteHadoopConfiguration(container);
            };

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        startAzuriteTest();
        writeTestFile("input/orders.csv", "id,name\n1,order-a\n2,order-b\n");
        writeTestFile("input/customers.csv", "id,name\n3,customer-a\n4,customer-b\n");
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            if (blobContainer != null) {
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
        String engine = container.identifier().name();
        String runId = engine.toLowerCase(Locale.ROOT).replace('_', '-');
        List<String> variables = Arrays.asList("RUN_ID=" + runId);
        String outputDirectory = "output/" + runId;
        String temporaryDirectory = "tmp/" + runId;
        String staleFile = outputDirectory + "/stale.csv";

        // DROP_DATA must remove this object before the sink commits its new output.
        writeTestFile(staleFile, "id,name\n99,stale\n");

        Container.ExecResult writeResult = container.executeJob(AZURITE_WRITE_JOB, variables);
        Assertions.assertEquals(0, writeResult.getExitCode(), writeResult.getStderr());

        Assertions.assertTrue(
                containsFiles(outputDirectory), "WASB output was not created for " + engine);
        Assertions.assertFalse(
                exists(staleFile), "DROP_DATA did not remove the stale object for " + engine);
        List<String> temporaryPayloads = listPayloadBlobs(temporaryDirectory);
        Assertions.assertTrue(
                temporaryPayloads.isEmpty(),
                "The sink did not rename and clean up its temporary output for "
                        + engine
                        + "; remaining payload blobs: "
                        + temporaryPayloads);

        Container.ExecResult readResult = container.executeJob(AZURITE_READ_JOB, variables);
        Assertions.assertEquals(
                0,
                readResult.getExitCode(),
                "WASB read-back failed for " + engine + ": " + readResult.getStderr());
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
        blobContainer.getBlockBlobReference(blobName(relativePath)).uploadText(contents);
    }

    private boolean exists(String relativePath) throws Exception {
        return blobContainer.getBlockBlobReference(blobName(relativePath)).exists();
    }

    private boolean containsFiles(String relativePath) throws Exception {
        return !listPayloadBlobs(relativePath).isEmpty();
    }

    /**
     * Returns actual Azurite payload blobs below a prefix, excluding WASB directory marker blobs.
     * Directory markers are zero-length metadata objects and are not transaction output.
     */
    private List<String> listPayloadBlobs(String relativePath) throws Exception {
        List<String> payloads = new ArrayList<>();
        for (ListBlobItem item : blobContainer.listBlobs(blobName(relativePath) + "/", true)) {
            if (!(item instanceof CloudBlob)) {
                continue;
            }
            CloudBlob blob = (CloudBlob) item;
            blob.downloadAttributes();
            Map<String, String> metadata = blob.getMetadata();
            if (metadata != null && "true".equalsIgnoreCase(metadata.get("hdi_isfolder"))) {
                continue;
            }
            payloads.add(
                    blob.getName()
                            + " ("
                            + blob.getProperties().getBlobType()
                            + ", "
                            + blob.getProperties().getLength()
                            + " bytes)");
        }
        return payloads;
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

    /** Installs the legacy Blob filesystem settings only in the Azurite E2E containers. */
    private static void copyAzuriteHadoopConfiguration(GenericContainer<?> container)
            throws IOException, InterruptedException {
        String accountHost = AZURITE_ACCOUNT + "." + AZURITE_ENDPOINT_SUFFIX;
        String contents =
                "<?xml version=\"1.0\"?>\n"
                        + "<configuration>\n"
                        + "  <property>\n"
                        + "    <name>fs.wasb.impl</name>\n"
                        + "    <value>org.apache.hadoop.fs.azure.NativeAzureFileSystem</value>\n"
                        + "  </property>\n"
                        + "  <property>\n"
                        + "    <name>fs.wasb.impl.disable.cache</name>\n"
                        + "    <value>true</value>\n"
                        + "  </property>\n"
                        + "  <property>\n"
                        + "    <name>fs.azure.account.key."
                        + accountHost
                        + "</name>\n"
                        + "    <value>"
                        + AZURITE_ACCOUNT_KEY
                        + "</value>\n"
                        + "  </property>\n"
                        + "</configuration>\n";

        java.nio.file.Path hadoopConfig = Files.createTempFile("seatunnel-adls-azurite-", ".xml");
        try {
            Files.write(hadoopConfig, contents.getBytes(StandardCharsets.UTF_8));
            container.copyFileToContainer(
                    MountableFile.forHostPath(hadoopConfig), AZURITE_HADOOP_CONFIG_PATH);
            Container.ExecResult chmod =
                    container.execInContainer("chmod", "600", AZURITE_HADOOP_CONFIG_PATH);
            Assertions.assertEquals(0, chmod.getExitCode(), chmod.getStderr());
        } finally {
            Files.deleteIfExists(hadoopConfig);
        }
    }
}
