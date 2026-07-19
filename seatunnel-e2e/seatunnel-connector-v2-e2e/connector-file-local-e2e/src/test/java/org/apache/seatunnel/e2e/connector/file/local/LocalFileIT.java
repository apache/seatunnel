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

package org.apache.seatunnel.e2e.connector.file.local;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;
import org.apache.seatunnel.connectors.seatunnel.file.local.catalog.LocalFileCatalog;
import org.apache.seatunnel.connectors.seatunnel.file.local.config.LocalFileHadoopConf;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.shaded.com.github.dockerjava.core.command.ExecStartResultCallback;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import io.airlift.compress.lzo.LzopCodec;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "The apache-compress version is not compatible with apache-poi")
@Slf4j
public class LocalFileIT extends TestSuiteBase {

    private GenericContainer<?> baseContainer;

    /** Copy data files to container */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                this.baseContainer = container;

                Path xlsGz =
                        convertToGzFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/excel/e2e.xls")),
                                "e2e-gz.xls");
                ContainerUtil.copyFileIntoContainers(
                        xlsGz, "/seatunnel/read/gz/excel/single/e2e-gz.xls.gz", container);

                Path xlsxGz =
                        convertToGzFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/excel/e2e.xlsx")),
                                "e2e-gz.xlsx");
                ContainerUtil.copyFileIntoContainers(
                        xlsxGz, "/seatunnel/read/gz/excel/single/e2e-gz.xlsx.gz", container);

                ContainerUtil.copyFileIntoContainers(
                        "/json/e2e.json",
                        "/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/json/e2e_gbk.json",
                        "/seatunnel/read/encoding/json/e2e_gbk.json",
                        container);

                Path jsonLzo = convertToLzoFile(ContainerUtil.getResourcesFile("/json/e2e.json"));
                ContainerUtil.copyFileIntoContainers(
                        jsonLzo, "/seatunnel/read/lzo_json/e2e.json", container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt",
                        "/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                        container);

                Path txtZip =
                        convertToZipFile(
                                Lists.newArrayList(ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "e2e-txt");
                ContainerUtil.copyFileIntoContainers(
                        txtZip, "/seatunnel/read/zip/txt/single/e2e-txt.zip", container);

                Path multiTxtZip =
                        convertToZipFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/text/e2e.txt"),
                                        ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "multiZip");
                ContainerUtil.copyFileIntoContainers(
                        multiTxtZip, "/seatunnel/read/zip/txt/multifile/multiZip.zip", container);

                Path txtTar =
                        convertToTarFile(
                                Lists.newArrayList(ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "e2e-txt");
                ContainerUtil.copyFileIntoContainers(
                        txtTar, "/seatunnel/read/tar/txt/single/e2e-txt.tar", container);

                Path multiTxtTar =
                        convertToTarFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/text/e2e.txt"),
                                        ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "multiTar");
                ContainerUtil.copyFileIntoContainers(
                        multiTxtTar, "/seatunnel/read/tar/txt/multifile/multiTar.tar", container);

                Path txtTarGz =
                        convertToTarGzFile(
                                Lists.newArrayList(ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "e2e-txt");
                ContainerUtil.copyFileIntoContainers(
                        txtTarGz, "/seatunnel/read/tar_gz/txt/single/e2e-txt.tar.gz", container);

                Path multiTxtTarGz =
                        convertToTarGzFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/text/e2e.txt"),
                                        ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "multiTarGz");
                ContainerUtil.copyFileIntoContainers(
                        multiTxtTarGz,
                        "/seatunnel/read/tar_gz/txt/multifile/multiTarGz.tar.gz",
                        container);

                Path txtGz =
                        convertToGzFile(
                                Lists.newArrayList(ContainerUtil.getResourcesFile("/text/e2e.txt")),
                                "e2e-txt-gz");
                ContainerUtil.copyFileIntoContainers(
                        txtGz, "/seatunnel/read/gz/txt/single/e2e-txt-gz.gz", container);

                Path jsonZip =
                        convertToZipFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/json/e2e.json")),
                                "e2e-json");
                ContainerUtil.copyFileIntoContainers(
                        jsonZip, "/seatunnel/read/zip/json/single/e2e-json.zip", container);

                Path multiJsonZip =
                        convertToZipFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/json/e2e.json"),
                                        ContainerUtil.getResourcesFile("/json/e2e.json")),
                                "multiJson");
                ContainerUtil.copyFileIntoContainers(
                        multiJsonZip,
                        "/seatunnel/read/zip/json/multifile/multiJson.zip",
                        container);

                Path jsonGz =
                        convertToGzFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/json/e2e.json")),
                                "e2e-json-gz");
                ContainerUtil.copyFileIntoContainers(
                        jsonGz, "/seatunnel/read/gz/json/single/e2e-json-gz.gz", container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e_gbk.txt",
                        "/seatunnel/read/encoding/text/e2e_gbk.txt",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e_delimiter.txt",
                        "/seatunnel/read/text_delimiter/e2e.txt",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e_time_format.txt",
                        "/seatunnel/read/text_time_format/e2e.txt",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/xml/e2e.xml", "/seatunnel/read/xml/e2e.xml", container);

                Path xmlZip =
                        convertToZipFile(
                                Lists.newArrayList(ContainerUtil.getResourcesFile("/xml/e2e.xml")),
                                "e2e-xml");
                ContainerUtil.copyFileIntoContainers(
                        xmlZip, "/seatunnel/read/zip/xml/single/e2e-xml.zip", container);

                Path xmlGz =
                        convertToGzFile(
                                Lists.newArrayList(ContainerUtil.getResourcesFile("/xml/e2e.xml")),
                                "e2e-xml-gz");
                ContainerUtil.copyFileIntoContainers(
                        xmlGz, "/seatunnel/read/gz/xml/single/e2e-xml-gz.gz", container);

                Path txtLzo = convertToLzoFile(ContainerUtil.getResourcesFile("/text/e2e.txt"));
                ContainerUtil.copyFileIntoContainers(
                        txtLzo, "/seatunnel/read/lzo_text/e2e.txt", container);
                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xlsx",
                        "/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                        container);
                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xls",
                        "/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xls",
                        container);

                Path xlsxZip =
                        convertToZipFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/excel/e2e.xlsx")),
                                "e2e-txt");
                ContainerUtil.copyFileIntoContainers(
                        xlsxZip, "/seatunnel/read/zip/excel/single/e2e-xlsx.zip", container);

                Path multiXlsxZip =
                        convertToZipFile(
                                Lists.newArrayList(
                                        ContainerUtil.getResourcesFile("/excel/e2e.xlsx"),
                                        ContainerUtil.getResourcesFile("/excel/e2e.xlsx")),
                                "multiXlsxZip");
                ContainerUtil.copyFileIntoContainers(
                        multiXlsxZip,
                        "/seatunnel/read/zip/excel/multifile/multiZip.zip",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/orc/e2e.orc",
                        "/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/orc/orc_for_cast.orc", "/seatunnel/read/orc_cast/e2e.orc", container);

                ContainerUtil.copyFileIntoContainers(
                        "/parquet/e2e.parquet",
                        "/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/binary/cat.png", "/seatunnel/read/binary/cat.png", container);

                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xlsx",
                        "/seatunnel/read/excel_filter/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xlsx",
                        "/seatunnel/read/excel_filter_regex/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/excel/special_excel.xlsx",
                        "/seatunnel/read/special_excel/special_excel.xlsx",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/csv/break_line.csv",
                        "/seatunnel/read/csv/break_line/break_line.csv",
                        container);
                ContainerUtil.copyFileIntoContainers(
                        "/csv/csv_with_header1.csv",
                        "/seatunnel/read/csv/header/csv_with_header1.csv",
                        container);
                ContainerUtil.copyFileIntoContainers(
                        "/csv/csv_with_header2.csv",
                        "/seatunnel/read/csv/header/csv_with_header2.csv",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e_null_format.txt",
                        "/seatunnel/read/e2e_null_format/e2e_null_format.txt",
                        container);

                container.execInContainer(
                        "sh",
                        "-c",
                        "mkdir -p /seatunnel/read/markdown && printf '# E2E Markdown RAG\\n' > /seatunnel/read/markdown/e2e.md");

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt", "/seatunnel/read/recursive/e2e.txt", container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt", "/seatunnel/read/recursive/subdir/e2e.txt", container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt",
                        "/seatunnel/read/recursive/subdir/deeper/e2e.txt",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt",
                        "/seatunnel/read/recursive/subdir/deeper/final/e2e.txt",
                        container);

                container.execInContainer("mkdir", "-p", "/tmp/fake_empty");
            };

    @TestTemplate
    public void testLocalFileCsv(TestContainer container) throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/csv/fake_to_local_csv.conf");
        helper.execute("/csv/local_csv_to_assert.conf");
        helper.execute("/csv/local_csv_enable_split_to_assert.conf");
        helper.execute("/csv/csv_with_header_to_assert.conf");
        helper.execute("/csv/breakline_csv_to_assert.conf");
    }

    @TestTemplate
    public void testLocalFileExcel(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/excel/fake_to_local_excel.conf");
        helper.execute("/excel/local_excel_to_assert.conf");
        helper.execute("/excel/local_excel_projection_to_assert.conf");
        helper.execute("/excel/special_excel_to_assert.conf");
        helper.execute("/excel/local_filter_excel_to_assert.conf");
        helper.execute("/excel/local_filter_regex_excel_to_assert.conf");
    }

    @TestTemplate
    public void testLocalFileText(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/text/fake_to_local_file_text.conf");
        helper.execute("/text/local_file_text_lzo_to_assert.conf");
        helper.execute("/text/local_file_delimiter_assert.conf");
        helper.execute("/text/local_file_time_format_assert.conf");
        helper.execute("/text/local_file_text_skip_headers.conf");
        helper.execute("/text/local_file_text_to_assert.conf");
        helper.execute("/text/local_file_text_projection_to_assert.conf");
        helper.execute("/text/fake_to_local_file_with_encoding.conf");
        helper.execute("/text/local_file_text_to_console_with_encoding.conf");
        helper.execute("/text/local_file_null_format_assert.conf");
    }

    @TestTemplate
    public void testLocalFileJson(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/json/fake_to_local_file_json.conf");
        helper.execute("/json/local_file_json_to_assert.conf");
        helper.execute("/json/local_file_json_enable_split_to_assert.conf");
        helper.execute("/json/local_file_json_lzo_to_console.conf");
        helper.execute("/json/fake_to_local_file_json_with_encoding.conf");
        helper.execute("/json/local_file_json_to_console_with_encoding.conf");
        helper.execute("/json/local_file_to_console.conf");
    }

    @TestTemplate
    public void testLocalFileOrcParquetBinaryXml(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/orc/fake_to_local_file_orc.conf");
        helper.execute("/orc/local_file_orc_to_assert.conf");
        helper.execute("/orc/local_file_orc_projection_to_assert.conf");
        helper.execute("/orc/local_file_orc_to_assert_with_time_and_cast.conf");
        helper.execute("/parquet/fake_to_local_file_parquet.conf");
        helper.execute("/parquet/local_file_parquet_to_assert.conf");
        helper.execute("/parquet/local_file_parquet_enable_split_to_assert.conf");
        helper.execute("/parquet/local_file_parquet_projection_to_assert.conf");
        helper.execute("/parquet/local_file_to_console.conf");
        helper.execute("/binary/local_file_binary_to_local_file_binary.conf");
        if (!container.identifier().getEngineType().equals(EngineType.FLINK)) {
            helper.execute("/binary/local_file_binary_to_assert.conf");
        }
        helper.execute("/xml/local_file_xml_to_assert.conf");
    }

    @TestTemplate
    public void testLocalFileCompressed(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/text/local_file_zip_text_to_assert.conf");
        helper.execute("/text/local_file_gz_text_to_assert.conf");
        helper.execute("/text/local_file_multi_zip_text_to_assert.conf");
        helper.execute("/text/local_file_tar_text_to_assert.conf");
        helper.execute("/text/local_file_text_enable_split_to_assert.conf");
        helper.execute("/text/local_file_multi_tar_text_to_assert.conf");
        helper.execute("/text/local_file_tar_gz_text_to_assert.conf");
        helper.execute("/text/local_file_multi_tar_gz_text_to_assert.conf");
        helper.execute("/json/local_file_json_zip_to_assert.conf");
        helper.execute("/json/local_file_json_gz_to_assert.conf");
        helper.execute("/json/local_file_json_multi_zip_to_assert.conf");
        helper.execute("/xml/local_file_zip_xml_to_assert.conf");
        helper.execute("/xml/local_file_gz_xml_to_assert.conf");
        helper.execute("/excel/local_excel_zip_to_assert.conf");
        helper.execute("/excel/local_excel_multi_zip_to_assert.conf");
        helper.execute("/excel/local_excel_xls_gz_to_assert.conf");
        helper.execute("/excel/local_excel_xlsx_gz_to_assert.conf");

        // test read recursive file path
        helper.execute("/text/local_file_text_recursive_to_assert.conf");
        helper.execute("/text/local_file_text_non_recursive_to_assert.conf");
    }

    @TestTemplate
    public void testLocalFileReadMarkdownRagMetadata(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/markdown/local_file_markdown_rag_metadata_to_assert.conf");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "sync_mode=update needs to compare source/target on the same filesystem. Local filesystem is not shared between engine master/workers in Flink/Spark E2E.")
    public void testLocalFileBinaryUpdateModeDistcp(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putLocalFile("/tmp/seatunnel/update/src/test.bin", "abc");

        TestHelper helper = new TestHelper(container);
        helper.execute("/binary/local_file_binary_update_distcp.conf");
        Assertions.assertEquals("abc", readLocalFile("/tmp/seatunnel/update/dst/test.bin"));

        // Make target newer with same length, distcp strategy should SKIP overwrite.
        putLocalFile("/tmp/seatunnel/update/dst/test.bin", "zzz");
        helper.execute("/binary/local_file_binary_update_distcp.conf");
        Assertions.assertEquals("zzz", readLocalFile("/tmp/seatunnel/update/dst/test.bin"));

        // Change source length, distcp strategy should COPY overwrite.
        putLocalFile("/tmp/seatunnel/update/src/test.bin", "abcd");
        helper.execute("/binary/local_file_binary_update_distcp.conf");
        Assertions.assertEquals("abcd", readLocalFile("/tmp/seatunnel/update/dst/test.bin"));

        baseContainer.execInContainer("sh", "-c", "rm -rf /tmp/seatunnel/update");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "sync_mode=update needs to compare source/target on the same filesystem. Local filesystem is not shared between engine master/workers in Flink/Spark E2E.")
    public void testLocalFileBinaryUpdateModeStrictChecksum(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putLocalFile("/tmp/seatunnel/update/src/test.bin", "abc");

        TestHelper helper = new TestHelper(container);
        helper.execute("/binary/local_file_binary_update_strict_checksum.conf");
        Assertions.assertEquals("abc", readLocalFile("/tmp/seatunnel/update/dst/test.bin"));

        long firstMtimeSeconds = getLocalFileMtimeSeconds("/tmp/seatunnel/update/dst/test.bin");
        Thread.sleep(1100);

        helper.execute("/binary/local_file_binary_update_strict_checksum.conf");
        long secondMtimeSeconds = getLocalFileMtimeSeconds("/tmp/seatunnel/update/dst/test.bin");
        Assertions.assertEquals(
                firstMtimeSeconds,
                secondMtimeSeconds,
                "Strict checksum should skip unchanged files and keep target mtime");

        baseContainer.execInContainer("sh", "-c", "rm -rf /tmp/seatunnel/update");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "Continuous discovery is a long-running job. Local filesystem is not shared between engine master/workers in Flink/Spark E2E.")
    public void testLocalFileBinaryUpdateModeContinuousDiscovery(TestContainer container)
            throws IOException, InterruptedException {
        resetContinuousTestPath();
        putLocalFile("/tmp/seatunnel/continuous/src/test1.bin", "abc");

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/binary/local_file_binary_update_distcp_continuous.conf",
                                        jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "abc",
                                        readLocalFile("/tmp/seatunnel/continuous/dst/test1.bin")));

        long firstMtimeSeconds =
                getLocalFileMtimeSeconds("/tmp/seatunnel/continuous/dst/test1.bin");
        Thread.sleep(2500);
        long secondMtimeSeconds =
                getLocalFileMtimeSeconds("/tmp/seatunnel/continuous/dst/test1.bin");
        Assertions.assertEquals(
                firstMtimeSeconds,
                secondMtimeSeconds,
                "Continuous discovery should skip unchanged files in update mode.");

        putLocalFile("/tmp/seatunnel/continuous/src/test2.bin", "def");
        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "def",
                                        readLocalFile("/tmp/seatunnel/continuous/dst/test2.bin")));

        Container.ExecResult cancelResult = container.cancelJob(jobId);
        Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

        Container.ExecResult execResult;
        try {
            execResult = jobFuture.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous job exit failed.", e);
        }
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        baseContainer.execInContainer("sh", "-c", "rm -rf /tmp/seatunnel/continuous");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "Continuous discovery is a long-running job. Local filesystem is not shared between engine master/workers in Flink/Spark E2E.")
    public void testLocalFileBinaryUpdateModeContinuousDiscoveryWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetContinuousTestPath();

        String jobId = String.valueOf(JobIdGenerator.newJobId());
        CompletableFuture<Container.ExecResult> jobFuture =
                CompletableFuture.supplyAsync(
                        () -> {
                            try {
                                return container.executeJob(
                                        "/binary/local_file_binary_update_distcp_continuous_non_recursive.conf",
                                        jobId);
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });

        putLocalFile("/tmp/seatunnel/continuous/src/root.bin", "root");
        putLocalFile("/tmp/seatunnel/continuous/src/subdir/nested.bin", "nested");

        Awaitility.await()
                .atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        "root",
                                        readLocalFile("/tmp/seatunnel/continuous/dst/root.bin")));

        Thread.sleep(3000);
        Assertions.assertFalse(
                isLocalFileExists("/tmp/seatunnel/continuous/dst/subdir/nested.bin"));

        Container.ExecResult cancelResult = container.cancelJob(jobId);
        Assertions.assertEquals(0, cancelResult.getExitCode(), cancelResult.getStderr());

        Container.ExecResult execResult;
        try {
            execResult = jobFuture.get(120, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException("Wait continuous job exit failed.", e);
        }
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        baseContainer.execInContainer("sh", "-c", "rm -rf /tmp/seatunnel/continuous");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "sync_mode=update needs to compare source/target on the same filesystem. Local filesystem is not shared between engine master/workers in Flink/Spark E2E.")
    public void testLocalFileBinaryUpdateModeDistcpWithNonRecursiveScan(TestContainer container)
            throws IOException, InterruptedException {
        resetUpdateTestPath();
        putLocalFile("/tmp/seatunnel/update/src/root.bin", "root-updated-v2");
        putLocalFile("/tmp/seatunnel/update/src/subdir/nested.bin", "nest-updated-v2");
        putLocalFile("/tmp/seatunnel/update/dst/root.bin", "root-stale-v1");
        putLocalFile("/tmp/seatunnel/update/dst/subdir/nested.bin", "nest-stale-v1");

        TestHelper helper = new TestHelper(container);
        helper.execute("/binary/local_file_binary_update_non_recursive_distcp.conf");

        Assertions.assertEquals(
                "root-updated-v2", readLocalFile("/tmp/seatunnel/update/dst/root.bin"));
        Assertions.assertEquals(
                "nest-stale-v1", readLocalFile("/tmp/seatunnel/update/dst/subdir/nested.bin"));

        baseContainer.execInContainer("sh", "-c", "rm -rf /tmp/seatunnel/update");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK},
            disabledReason =
                    "sync_mode=update needs to compare source/target on the same filesystem. Local filesystem is not shared between engine master/workers in Flink/Spark E2E.")
    public void testLocalFileBinaryUpdateModeStrictChecksumSkipsNestedChangesWithNonRecursiveScan(
            TestContainer container) throws IOException, InterruptedException {
        resetUpdateTestPath();
        putLocalFile("/tmp/seatunnel/update/src/root.bin", "root-same-v1");
        putLocalFile("/tmp/seatunnel/update/src/subdir/nested.bin", "nest-new-v1");
        putLocalFile("/tmp/seatunnel/update/dst/root.bin", "root-same-v1");
        putLocalFile("/tmp/seatunnel/update/dst/subdir/nested.bin", "nest-old-v1");

        TestHelper helper = new TestHelper(container);
        helper.execute("/binary/local_file_binary_update_non_recursive_strict_checksum.conf");

        Assertions.assertEquals(
                "root-same-v1", readLocalFile("/tmp/seatunnel/update/dst/root.bin"));
        Assertions.assertEquals(
                "nest-old-v1", readLocalFile("/tmp/seatunnel/update/dst/subdir/nested.bin"));

        baseContainer.execInContainer("sh", "-c", "rm -rf /tmp/seatunnel/update");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {TestContainerId.SPARK_2_4},
            type = {EngineType.FLINK},
            disabledReason =
                    "Fink test is multi-node, LocalFile connector will use different containers for obtaining files")
    public void testLocalFileReadAndWriteWithSaveMode(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        // test save_mode
        String path = "/tmp/seatunnel/localfile/json/fake";
        Assertions.assertEquals(getFileListFromContainer(path).size(), 0);
        helper.execute("/json/fake_to_local_file_json_save_mode.conf");
        Assertions.assertEquals(getFileListFromContainer(path).size(), 1);
        helper.execute("/json/fake_to_local_file_json_save_mode.conf");
        Assertions.assertEquals(getFileListFromContainer(path).size(), 1);
    }

    @SneakyThrows
    private List<String> getFileListFromContainer(String path) {
        String command = "ls -1 " + path;
        ExecCreateCmdResponse execCreateCmdResponse =
                dockerClient
                        .execCreateCmd(baseContainer.getContainerId())
                        .withCmd("sh", "-c", command)
                        .withAttachStdout(true)
                        .withAttachStderr(true)
                        .exec();

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        dockerClient
                .execStartCmd(execCreateCmdResponse.getId())
                .exec(new ExecStartResultCallback(outputStream, System.err))
                .awaitCompletion();

        String output = new String(outputStream.toByteArray(), StandardCharsets.UTF_8).trim();
        List<String> fileList = new ArrayList<>();
        log.info("container path file list is :{}", output);
        String[] files = output.split("\n");
        for (String file : files) {
            if (StringUtils.isNotEmpty(file)) {
                log.info("container path file name is :{}", file);
                fileList.add(file);
            }
        }
        return fileList;
    }

    @TestTemplate
    public void testLocalFileCatalog(TestContainer container)
            throws IOException, InterruptedException {
        final LocalFileCatalog localFileCatalog =
                new LocalFileCatalog(
                        new HadoopFileSystemProxy(new LocalFileHadoopConf()),
                        "/tmp/seatunnel/json/test1",
                        FileSystemType.LOCAL.getFileSystemPluginName());
        final TablePath tablePath = TablePath.DEFAULT;
        Assertions.assertFalse(localFileCatalog.tableExists(tablePath));
        localFileCatalog.createTable(null, null, false);
        Assertions.assertTrue(localFileCatalog.tableExists(tablePath));
        Assertions.assertFalse(localFileCatalog.isExistsData(tablePath));
        localFileCatalog.dropTable(tablePath, false);
        Assertions.assertFalse(localFileCatalog.tableExists(tablePath));
    }

    private void resetUpdateTestPath() throws IOException, InterruptedException {
        Container.ExecResult result =
                baseContainer.execInContainer(
                        "sh",
                        "-c",
                        "rm -rf /tmp/seatunnel/update && mkdir -p /tmp/seatunnel/update/src /tmp/seatunnel/update/dst /tmp/seatunnel/update/tmp");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private void resetContinuousTestPath() throws IOException, InterruptedException {
        Container.ExecResult result =
                baseContainer.execInContainer(
                        "sh",
                        "-c",
                        "rm -rf /tmp/seatunnel/continuous && mkdir -p /tmp/seatunnel/continuous/src /tmp/seatunnel/continuous/dst /tmp/seatunnel/continuous/tmp");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private void putLocalFile(String filePath, String content)
            throws IOException, InterruptedException {
        String command =
                "mkdir -p $(dirname '"
                        + filePath
                        + "') && printf '"
                        + content
                        + "' > '"
                        + filePath
                        + "' && chmod 666 '"
                        + filePath
                        + "'";
        Container.ExecResult result = baseContainer.execInContainer("sh", "-c", command);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private String readLocalFile(String filePath) throws IOException, InterruptedException {
        Container.ExecResult result =
                baseContainer.execInContainer("sh", "-c", "cat '" + filePath + "'");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        return result.getStdout() == null ? "" : result.getStdout().trim();
    }

    private boolean isLocalFileExists(String filePath) throws IOException, InterruptedException {
        Container.ExecResult result =
                baseContainer.execInContainer("sh", "-c", "test -f '" + filePath + "'");
        return result.getExitCode() == 0;
    }

    private long getLocalFileMtimeSeconds(String filePath)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                baseContainer.execInContainer("sh", "-c", "stat -c %Y '" + filePath + "'");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        return Long.parseLong(result.getStdout().trim());
    }

    private Path convertToLzoFile(File file) throws IOException {
        LzopCodec lzo = new LzopCodec();
        Path path = Paths.get(file.getAbsolutePath() + ".lzo");
        OutputStream outputStream = lzo.createOutputStream(Files.newOutputStream(path));
        outputStream.write(Files.readAllBytes(file.toPath()));
        outputStream.close();
        return path;
    }

    public Path convertToZipFile(List<File> files, String name) throws IOException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("File list is empty or invalid");
        }

        File firstFile = files.get(0);
        Path zipFilePath = Paths.get(firstFile.getParent(), String.format("%s.zip", name));

        try (ZipOutputStream zos = new ZipOutputStream(Files.newOutputStream(zipFilePath))) {
            for (File file : files) {
                if (file.isDirectory()) {
                    Path dirPath = file.toPath();
                    Files.walkFileTree(
                            dirPath,
                            new SimpleFileVisitor<Path>() {
                                @Override
                                public FileVisitResult visitFile(
                                        Path file, BasicFileAttributes attrs) throws IOException {
                                    addToZipFile(file, dirPath.getParent(), zos);
                                    return FileVisitResult.CONTINUE;
                                }
                            });
                } else {
                    addToZipFile(file.toPath(), file.getParentFile().toPath(), zos);
                }
            }
        }

        return zipFilePath;
    }

    private void addToZipFile(Path file, Path baseDir, ZipOutputStream zos) throws IOException {
        Path relativePath = baseDir.relativize(file);
        ZipEntry zipEntry;

        if (relativePath.toString().contains(".")) {
            String fileName = relativePath.toString().split("\\.")[0];
            String suffix = relativePath.toString().split("\\.")[1];
            zipEntry =
                    new ZipEntry(
                            new Random().nextInt()
                                    + fileName
                                    + "_"
                                    + System.currentTimeMillis()
                                    + "."
                                    + suffix);
            zos.putNextEntry(zipEntry);
        }
        Files.copy(file, zos);
        zos.closeEntry();
    }

    public Path convertToTarFile(List<File> files, String name) throws IOException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("File list is empty or invalid");
        }

        File firstFile = files.get(0);
        Path tarFilePath = Paths.get(firstFile.getParent(), String.format("%s.tar", name));

        try (TarArchiveOutputStream tarOut =
                new TarArchiveOutputStream(Files.newOutputStream(tarFilePath))) {
            for (File file : files) {
                if (file.isDirectory()) {
                    Path dirPath = file.toPath();
                    Files.walkFileTree(
                            dirPath,
                            new SimpleFileVisitor<Path>() {
                                @Override
                                public FileVisitResult visitFile(
                                        Path file, BasicFileAttributes attrs) throws IOException {
                                    addToTarFile(file, dirPath.getParent(), tarOut);
                                    return FileVisitResult.CONTINUE;
                                }
                            });
                } else {
                    addToTarFile(file.toPath(), file.getParentFile().toPath(), tarOut);
                }
            }
        }

        return tarFilePath;
    }

    private void addToTarFile(Path file, Path baseDir, TarArchiveOutputStream tarOut)
            throws IOException {
        Path relativePath = baseDir.relativize(file);

        TarArchiveEntry tarEntry;
        if (relativePath.toString().contains(".")) {
            String fileName = relativePath.toString().split("\\.")[0];
            String suffix = relativePath.toString().split("\\.")[1];
            String entryName =
                    new Random().nextInt()
                            + fileName
                            + "_"
                            + System.currentTimeMillis()
                            + "."
                            + suffix;
            tarEntry = new TarArchiveEntry(file.toFile(), entryName);
        } else {
            tarEntry = new TarArchiveEntry(file.toFile(), relativePath.toString());
        }

        tarOut.putArchiveEntry(tarEntry);
        Files.copy(file, tarOut);
        tarOut.closeArchiveEntry();
    }

    public Path convertToTarGzFile(List<File> files, String name) throws IOException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("File list is empty or invalid");
        }

        File firstFile = files.get(0);
        Path tarGzFilePath = Paths.get(firstFile.getParent(), String.format("%s.tar.gz", name));

        // Create a GZIP output stream wrapping the tar output stream
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(Files.newOutputStream(tarGzFilePath));
                TarArchiveOutputStream tarOut = new TarArchiveOutputStream(gzipOut)) {

            for (File file : files) {
                if (file.isDirectory()) {
                    Path dirPath = file.toPath();
                    Files.walkFileTree(
                            dirPath,
                            new SimpleFileVisitor<Path>() {
                                @Override
                                public FileVisitResult visitFile(
                                        Path file, BasicFileAttributes attrs) throws IOException {
                                    addToTarFile(file, dirPath.getParent(), tarOut);
                                    return FileVisitResult.CONTINUE;
                                }
                            });
                } else {
                    addToTarFile(file.toPath(), file.getParentFile().toPath(), tarOut);
                }
            }
        }

        return tarGzFilePath;
    }

    public Path convertToGzFile(List<File> files, String name) throws IOException {
        if (files == null || files.isEmpty()) {
            throw new IllegalArgumentException("File list is empty or invalid");
        }

        File firstFile = files.get(0);
        Path gzFilePath = Paths.get(firstFile.getParent(), String.format("%s.gz", name));

        try (FileInputStream fis = new FileInputStream(firstFile);
                FileOutputStream fos = new FileOutputStream(gzFilePath.toFile());
                GZIPOutputStream gzos = new GZIPOutputStream(fos)) {

            byte[] buffer = new byte[2048];
            int length;

            while ((length = fis.read(buffer)) > 0) {
                gzos.write(buffer, 0, length);
            }
            gzos.finish();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return gzFilePath;
    }
}
