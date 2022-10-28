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
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.nio.file.Path;

public class LocalFileIT extends TestSuiteBase {

    /**
     * Copy data files to container
     */
    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory = container -> {
        Path jsonPath = ContainerUtil.getResourcesFile("/json/e2e.json").toPath();
        Path orcPath = ContainerUtil.getResourcesFile("/orc/e2e.orc").toPath();
        Path parquetPath = ContainerUtil.getResourcesFile("/parquet/e2e.parquet").toPath();
        Path textPath = ContainerUtil.getResourcesFile("/text/e2e.txt").toPath();
        container.copyFileToContainer(MountableFile.forHostPath(jsonPath), "/seatunnel/read/json/name=tyrantlucifer/hobby=coding/e2e.json");
        container.copyFileToContainer(MountableFile.forHostPath(orcPath), "/seatunnel/read/orc/name=tyrantlucifer/hobby=coding/e2e.orc");
        container.copyFileToContainer(MountableFile.forHostPath(parquetPath), "/seatunnel/read/parquet/name=tyrantlucifer/hobby=coding/e2e.parquet");
        container.copyFileToContainer(MountableFile.forHostPath(textPath), "/seatunnel/read/text/name=tyrantlucifer/hobby=coding/e2e.txt");
    };

    @TestTemplate
    public void testLocalFileReadAndWrite(TestContainer container) throws IOException, InterruptedException {
        // test write local text file
        Container.ExecResult textWriteResult = container.executeJob("/text/fake_to_local_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // test read local text file
        Container.ExecResult textReadResult = container.executeJob("/text/local_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());
        // test write local json file
        Container.ExecResult jsonWriteResult = container.executeJob("/json/fake_to_local_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        // test read local json file
        Container.ExecResult jsonReadResult = container.executeJob("/json/local_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());
        // test write local orc file
        Container.ExecResult orcWriteResult = container.executeJob("/orc/fake_to_local_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());
        // test read local orc file
        Container.ExecResult orcReadResult = container.executeJob("/orc/local_file_orc_to_assert.conf");
        Assertions.assertEquals(0, orcReadResult.getExitCode());
        // test write local parquet file
        Container.ExecResult parquetWriteResult = container.executeJob("/parquet/fake_to_local_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
        // test read local parquet file
        Container.ExecResult parquetReadResult = container.executeJob("/parquet/local_file_parquet_to_assert.conf");
        Assertions.assertEquals(0, parquetReadResult.getExitCode());
    }
}
