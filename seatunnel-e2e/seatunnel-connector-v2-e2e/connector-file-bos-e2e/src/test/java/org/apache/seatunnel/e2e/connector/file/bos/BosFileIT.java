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

package org.apache.seatunnel.e2e.connector.file.bos;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

@Disabled
public class BosFileIT extends TestSuiteBase {

    @TestTemplate
    public void testBosFileWriteAndRead(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult excelWriteResult =
                container.executeJob("/excel/fake_to_bos_excel.conf");
        Assertions.assertEquals(0, excelWriteResult.getExitCode(), excelWriteResult.getStderr());
        Container.ExecResult excelReadResult =
                container.executeJob("/excel/bos_excel_to_assert.conf");
        Assertions.assertEquals(0, excelReadResult.getExitCode(), excelReadResult.getStderr());

        Container.ExecResult textWriteResult =
                container.executeJob("/text/fake_to_bos_file_text.conf");
        Assertions.assertEquals(0, textWriteResult.getExitCode());
        Container.ExecResult textReadResult =
                container.executeJob("/text/bos_file_text_to_assert.conf");
        Assertions.assertEquals(0, textReadResult.getExitCode());

        Container.ExecResult jsonWriteResult =
                container.executeJob("/json/fake_to_bos_file_json.conf");
        Assertions.assertEquals(0, jsonWriteResult.getExitCode());
        Container.ExecResult jsonReadResult =
                container.executeJob("/json/bos_file_json_to_assert.conf");
        Assertions.assertEquals(0, jsonReadResult.getExitCode());

        Container.ExecResult orcWriteResult =
                container.executeJob("/orc/fake_to_bos_file_orc.conf");
        Assertions.assertEquals(0, orcWriteResult.getExitCode());
        Container.ExecResult orcReadResult =
                container.executeJob("/orc/bos_file_orc_to_assert.conf");
        Assertions.assertEquals(0, orcReadResult.getExitCode());

        Container.ExecResult parquetWriteResult =
                container.executeJob("/parquet/fake_to_bos_file_parquet.conf");
        Assertions.assertEquals(0, parquetWriteResult.getExitCode());
        Container.ExecResult parquetReadResult =
                container.executeJob("/parquet/bos_file_parquet_to_assert.conf");
        Assertions.assertEquals(0, parquetReadResult.getExitCode());
    }
}
