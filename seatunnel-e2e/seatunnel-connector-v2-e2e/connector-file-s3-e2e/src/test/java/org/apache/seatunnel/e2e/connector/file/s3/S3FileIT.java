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

package org.apache.seatunnel.e2e.connector.file.s3;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import io.airlift.compress.lzo.LzopCodec;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Disabled("have no s3 environment to run this test")
public class S3FileIT extends TestSuiteBase {

    public static final String S3_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar";
    public static final String HADOOP_S3_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/s3/lib && cd /tmp/seatunnel/plugins/s3/lib && curl -O "
                                        + S3_SDK_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());

                extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "cd /tmp/seatunnel/plugins/s3/lib && curl -O "
                                        + HADOOP_S3_DOWNLOAD);
                Assertions.assertEquals(0, extraCommands.getExitCode());
            };

    /** Copy data files to s3 */
    @TestTemplate
    public void testS3FileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        // Copy test files to s3
        S3Utils s3Utils = new S3Utils();
        try {
            s3Utils.uploadTestFiles(
                    "/json/e2e.json",
                    "test/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                    true);
            Path jsonLzo = convertToLzoFile(ContainerUtil.getResourcesFile("/json/e2e.json"));
            s3Utils.uploadTestFiles(
                    jsonLzo.toString(), "test/seatunnel/read/lzo_json/e2e.json", false);
            s3Utils.uploadTestFiles(
                    "/text/e2e.txt",
                    "test/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                    true);
            s3Utils.uploadTestFiles(
                    "/text/e2e_delimiter.txt", "test/seatunnel/read/text_delimiter/e2e.txt", true);
            s3Utils.uploadTestFiles(
                    "/text/e2e_time_format.txt",
                    "test/seatunnel/read/text_time_format/e2e.txt",
                    true);
            Path txtLzo = convertToLzoFile(ContainerUtil.getResourcesFile("/text/e2e.txt"));
            s3Utils.uploadTestFiles(
                    txtLzo.toString(), "test/seatunnel/read/lzo_text/e2e.txt", false);
            s3Utils.uploadTestFiles(
                    "/excel/e2e.xlsx",
                    "test/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                    true);
            s3Utils.uploadTestFiles(
                    "/orc/e2e.orc",
                    "test/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                    true);
            s3Utils.uploadTestFiles(
                    "/parquet/e2e.parquet",
                    "test/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                    true);
            s3Utils.uploadTestFiles(
                    "/excel/e2e.xlsx",
                    "test/seatunnel/read/excel_filter/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                    true);
            s3Utils.uploadTestFiles(
                    "/text/e2e-text.zip", "test/seatunnel/read/text_zip/e2e-text.zip", true);
            s3Utils.createDir("tmp/fake_empty");
        } finally {
            s3Utils.close();
        }

        TestHelper helper = new TestHelper(container);

        helper.execute("/text/s3_file_zip_text_to_assert.conf");
        helper.execute("/excel/fake_to_s3_excel.conf");
        helper.execute("/excel/s3_excel_to_assert.conf");
        helper.execute("/excel/s3_excel_projection_to_assert.conf");
        // test write s3 text file
        helper.execute("/text/fake_to_s3_file_text.conf");
        helper.execute("/text/s3_file_text_lzo_to_assert.conf");
        helper.execute("/text/s3_file_delimiter_assert.conf");
        helper.execute("/text/s3_file_time_format_assert.conf");
        // test read skip header
        helper.execute("/text/s3_file_text_skip_headers.conf");
        // test read s3 text file
        helper.execute("/text/s3_file_text_to_assert.conf");
        // test read s3 text file with projection
        helper.execute("/text/s3_file_text_projection_to_assert.conf");
        // test write s3 json file
        helper.execute("/json/fake_to_s3_file_json.conf");
        // test read s3 json file
        helper.execute("/json/s3_file_json_to_assert.conf");
        helper.execute("/json/s3_file_json_lzo_to_console.conf");
        // test write s3 orc file
        helper.execute("/orc/fake_to_s3_file_orc.conf");
        // test read s3 orc file
        helper.execute("/orc/s3_file_orc_to_assert.conf");
        // test read s3 orc file with projection
        helper.execute("/orc/s3_file_orc_projection_to_assert.conf");
        // test write s3 parquet file
        helper.execute("/parquet/fake_to_s3_file_parquet.conf");
        // test read s3 parquet file
        helper.execute("/parquet/s3_file_parquet_to_assert.conf");
        // test read s3 parquet file with projection
        helper.execute("/parquet/s3_file_parquet_projection_to_assert.conf");
        // test read filtered s3 file
        helper.execute("/excel/s3_filter_excel_to_assert.conf");

        // test read empty directory
        helper.execute("/json/s3_file_to_console.conf");
        helper.execute("/parquet/s3_file_to_console.conf");
    }

    private Path convertToLzoFile(File file) throws IOException {
        LzopCodec lzo = new LzopCodec();
        Path path = Paths.get(file.getAbsolutePath() + ".lzo");
        OutputStream outputStream = lzo.createOutputStream(Files.newOutputStream(path));
        outputStream.write(Files.readAllBytes(file.toPath()));
        outputStream.close();
        return path;
    }
}
