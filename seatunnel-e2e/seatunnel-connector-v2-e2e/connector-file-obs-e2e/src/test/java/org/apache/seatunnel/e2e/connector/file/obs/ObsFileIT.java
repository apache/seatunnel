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

package org.apache.seatunnel.e2e.connector.file.obs;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.flink.Flink13Container;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Disabled
public class ObsFileIT extends TestSuiteBase {

    @TestTemplate
    public void testLocalFileReadAndWrite(TestContainer container)
            throws IOException, InterruptedException {
        if (container instanceof Flink13Container) {
            return;
        }
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fake_to_obs_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());
        Container.ExecResult excelReadResult =
                container.executeJob("/excel/obs_excel_to_assert.conf");
        Assertions.assertEquals(0, excelReadResult.getExitCode(), excelReadResult.getStderr());
        Container.ExecResult excelProjectionReadResult =
                container.executeJob("/excel/obs_excel_projection_to_assert.conf");
        Assertions.assertEquals(
                0, excelReadResult.getExitCode(), excelProjectionReadResult.getStderr());
        // test write obs text file
        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_obs_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        // test read skip header
        Container.ExecResult textWriteAndSkipResult =
                container.executeJob("/text/obs_file_text_skip_headers.conf");
        Assertions.assertEquals(0, textWriteAndSkipResult.getExitCode());
        // test read obs text file
        Container.ExecResult textReadResult =
                container.executeJob("/text/obs_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());
        // test read obs text file with projection
        Container.ExecResult textProjectionResult =
                container.executeJob("/text/obs_file_text_projection_to_assert.conf");
        Assertions.assertEquals(0, textProjectionResult.getExitCode());
        // test write obs json file
        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_obs_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        // test read obs json file
        Container.ExecResult jsonReadResult =
                container.executeJob("/json/obs_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());
        // test write obs orc file
        Container.ExecResult orcWriteResult =
                container.executeJob("/orc/fake_to_obs_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());
        // test read obs orc file
        Container.ExecResult orcReadResult =
                container.executeJob("/orc/obs_file_orc_to_assert.conf");
        Assertions.assertEquals(0, orcReadResult.getExitCode());
        // test read obs orc file with projection
        Container.ExecResult orcProjectionResult =
                container.executeJob("/orc/obs_file_orc_projection_to_assert.conf");
        Assertions.assertEquals(0, orcProjectionResult.getExitCode());
        // test write obs parquet file
        Container.ExecResult parquetWriteResult =
                container.executeJob("/parquet/fake_to_obs_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
        // test read obs parquet file
        Container.ExecResult parquetReadResult =
                container.executeJob("/parquet/obs_file_parquet_to_assert.conf");
        Assertions.assertEquals(0, parquetReadResult.getExitCode());
        // test read obs parquet file with projection
        Container.ExecResult parquetProjectionResult =
                container.executeJob("/parquet/obs_file_parquet_projection_to_assert.conf");
        Assertions.assertEquals(0, parquetProjectionResult.getExitCode());
    }
}
