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

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.lang3.StringUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
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
                        "/text/e2e_null_format.txt",
                        "/seatunnel/read/e2e_null_format/e2e_null_format.txt",
                        container);

                container.execInContainer("mkdir", "-p", "/tmp/fake_empty");
            };

    @TestTemplate
    public void testLocalFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);
        helper.execute("/csv/fake_to_local_csv.conf");
        helper.execute("/csv/local_csv_to_assert.conf");
        helper.execute("/excel/fake_to_local_excel.conf");
        helper.execute("/excel/local_excel_to_assert.conf");
        helper.execute("/excel/local_excel_projection_to_assert.conf");
        // test write local text file
        helper.execute("/text/fake_to_local_file_text.conf");
        helper.execute("/text/local_file_text_lzo_to_assert.conf");
        helper.execute("/text/local_file_delimiter_assert.conf");
        helper.execute("/text/local_file_time_format_assert.conf");
        // test read skip header
        helper.execute("/text/local_file_text_skip_headers.conf");
        // test read local text file
        helper.execute("/text/local_file_text_to_assert.conf");
        // test read local text file with projection
        helper.execute("/text/local_file_text_projection_to_assert.conf");
        // test read local csv file with assigning encoding
        helper.execute("/text/fake_to_local_file_with_encoding.conf");
        // test read local csv file with assigning encoding
        helper.execute("/text/local_file_text_to_console_with_encoding.conf");
        helper.execute("/text/local_file_null_format_assert.conf");

        // test write local json file
        helper.execute("/json/fake_to_local_file_json.conf");
        // test read local json file
        helper.execute("/json/local_file_json_to_assert.conf");
        helper.execute("/json/local_file_json_lzo_to_console.conf");
        // test read local json file with assigning encoding
        helper.execute("/json/fake_to_local_file_json_with_encoding.conf");
        // test write local json file with assigning encoding
        helper.execute("/json/local_file_json_to_console_with_encoding.conf");

        // test write local orc file
        helper.execute("/orc/fake_to_local_file_orc.conf");
        // test read local orc file
        helper.execute("/orc/local_file_orc_to_assert.conf");
        // test read local orc file with projection
        helper.execute("/orc/local_file_orc_projection_to_assert.conf");
        // test read local orc file with projection and type cast
        helper.execute("/orc/local_file_orc_to_assert_with_time_and_cast.conf");
        // test write local parquet file
        helper.execute("/parquet/fake_to_local_file_parquet.conf");
        // test read local parquet file
        helper.execute("/parquet/local_file_parquet_to_assert.conf");
        // test read local parquet file with projection
        helper.execute("/parquet/local_file_parquet_projection_to_assert.conf");
        // test read filtered local file
        helper.execute("/excel/local_filter_excel_to_assert.conf");

        // test read empty directory
        helper.execute("/json/local_file_to_console.conf");
        helper.execute("/parquet/local_file_to_console.conf");

        // test binary file
        helper.execute("/binary/local_file_binary_to_local_file_binary.conf");
        if (!container.identifier().getEngineType().equals(EngineType.FLINK)) {
            // the file generated by local_file_binary_to_local_file_binary in taskManager, so read
            // from jobManager will be failed in Flink
            helper.execute("/binary/local_file_binary_to_assert.conf");
        }

        helper.execute("/xml/local_file_xml_to_assert.conf");
        /** Compressed file test */
        // test read single local text file with zip compression
        helper.execute("/text/local_file_zip_text_to_assert.conf");
        helper.execute("/text/local_file_gz_text_to_assert.conf");
        // test read multi local text file with zip compression
        helper.execute("/text/local_file_multi_zip_text_to_assert.conf");
        // test read single local text file with tar compression
        helper.execute("/text/local_file_tar_text_to_assert.conf");
        // test read multi local text file with tar compression
        helper.execute("/text/local_file_multi_tar_text_to_assert.conf");
        // test read single local text file with tar.gz compression
        helper.execute("/text/local_file_tar_gz_text_to_assert.conf");
        // test read multi local text file with tar.gz compression
        helper.execute("/text/local_file_multi_tar_gz_text_to_assert.conf");
        // test read single local json file with zip compression
        helper.execute("/json/local_file_json_zip_to_assert.conf");
        helper.execute("/json/local_file_json_gz_to_assert.conf");
        // test read multi local json file with zip compression
        helper.execute("/json/local_file_json_multi_zip_to_assert.conf");
        // test read single local xml file with zip compression
        helper.execute("/xml/local_file_zip_xml_to_assert.conf");
        helper.execute("/xml/local_file_gz_xml_to_assert.conf");
        // test read single local excel file with zip compression
        helper.execute("/excel/local_excel_zip_to_assert.conf");
        // test read multi local excel file with zip compression
        helper.execute("/excel/local_excel_multi_zip_to_assert.conf");
        helper.execute("/excel/local_excel_xls_gz_to_assert.conf");
        helper.execute("/excel/local_excel_xlsx_gz_to_assert.conf");
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
