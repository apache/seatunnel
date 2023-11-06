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

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.TestHelper;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.TestTemplate;

import io.airlift.compress.lzo.LzopCodec;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@DisabledOnContainer(
        value = {TestContainerId.SPARK_2_4},
        type = {},
        disabledReason = "The apache-compress version is not compatible with apache-poi")
public class LocalFileIT extends TestSuiteBase {

    /** Copy data files to container */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                ContainerUtil.copyFileIntoContainers(
                        "/json/e2e.json",
                        "/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json",
                        container);

                Path jsonLzo = convertToLzoFile(ContainerUtil.getResourcesFile("/json/e2e.json"));
                ContainerUtil.copyFileIntoContainers(
                        jsonLzo, "/seatunnel/read/lzo_json/e2e.json", container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e.txt",
                        "/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/text/e2e_delimiter.txt",
                        "/seatunnel/read/text_delimiter/e2e.txt",
                        container);

                Path txtLzo = convertToLzoFile(ContainerUtil.getResourcesFile("/text/e2e.txt"));
                ContainerUtil.copyFileIntoContainers(
                        txtLzo, "/seatunnel/read/lzo_text/e2e.txt", container);
                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xlsx",
                        "/seatunnel/read/excel/name=tyrantlucifer/hobby=coding/e2e.xlsx",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/orc/e2e.orc",
                        "/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/parquet/e2e.parquet",
                        "/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet",
                        container);

                ContainerUtil.copyFileIntoContainers(
                        "/excel/e2e.xlsx",
                        "/seatunnel/read/excel_filter/name=tyrantlucifer/hobby=coding/e2e_filter.xlsx",
                        container);
                container.execInContainer("mkdir", "-p", "/tmp/fake_empty");
            };

    @TestTemplate
    public void testLocalFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        TestHelper helper = new TestHelper(container);

        helper.execute("/excel/fake_to_local_excel.conf");
        helper.execute("/excel/local_excel_to_assert.conf");
        helper.execute("/excel/local_excel_projection_to_assert.conf");
        // test write local text file
        helper.execute("/text/fake_to_local_file_text.conf");
        helper.execute("/text/local_file_text_lzo_to_assert.conf");
        helper.execute("/text/local_file_delimiter_assert.conf");
        // test read skip header
        helper.execute("/text/local_file_text_skip_headers.conf");
        // test read local text file
        helper.execute("/text/local_file_text_to_assert.conf");
        // test read local text file with projection
        helper.execute("/text/local_file_text_projection_to_assert.conf");
        // test write local json file
        helper.execute("/json/fake_to_local_file_json.conf");
        // test read local json file
        helper.execute("/json/local_file_json_to_assert.conf");
        helper.execute("/json/local_file_json_lzo_to_console.conf");
        // test write local orc file
        helper.execute("/orc/fake_to_local_file_orc.conf");
        // test read local orc file
        helper.execute("/orc/local_file_orc_to_assert.conf");
        // test read local orc file with projection
        helper.execute("/orc/local_file_orc_projection_to_assert.conf");
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
