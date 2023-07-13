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

package org.apache.seatunnel.e2e.connector.file.cos;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Disabled
public class CosFileIT extends TestSuiteBase {

    @TestTemplate
    public void testCosFileWriteAndRead(TestContainer container)
            throws IOException, InterruptedException {
        // test cos excel file
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fake_to_cos_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());
        Container.ExecResult excelReadResult =
                container.executeJob("/excel/cos_excel_to_assert.conf");
        Assertions.assertEquals(0, excelReadResult.getExitCode(), excelReadResult.getStderr());

        // test cos text file
        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_cos_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult textReadResult =
                container.executeJob("/text/cos_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());

        // test cos json file
        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_cos_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        Container.ExecResult jsonReadResult =
                container.executeJob("/json/cos_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());

        // test cos orc file
        Container.ExecResult orcWriteResult =
                container.executeJob("/orc/fake_to_cos_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());
        Container.ExecResult orcReadResult =
                container.executeJob("/orc/cos_file_orc_to_assert.conf");
        Assertions.assertEquals(0, orcReadResult.getExitCode());

        // test cos parquet file
        Container.ExecResult parquetWriteResult =
                container.executeJob("/parquet/fake_to_cos_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
        Container.ExecResult parquetReadResult =
                container.executeJob("/parquet/cos_file_parquet_to_assert.conf");
        Assertions.assertEquals(0, parquetReadResult.getExitCode());
    }
}
