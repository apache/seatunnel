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

package org.apache.seatunnel.connectors.seatunnel.file.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.writer.OrcWriteStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.OrcReadStrategy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;

@Slf4j
public class OrcWriteStrategyTest {
    private static final String TMP_PATH = "file:///tmp/seatunnel/orc/batch/test";
    private static final int ORC_WRITE_NUMBER = 2000;

    @DisabledOnOs(OS.WINDOWS)
    @Test
    public void testOrcWriteWithBatch() throws Exception {
        Map<String, Object> writeConfig = new HashMap<>();
        writeConfig.put("tmp_path", TMP_PATH);
        writeConfig.put("path", "file:///tmp/seatunnel/orc/batch");
        writeConfig.put("file_format_type", FileFormat.ORC.name());

        SeaTunnelRowType writeRowType =
                new SeaTunnelRowType(
                        new String[] {"f1_text"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE,
                        });
        FileSinkConfig writeSinkConfig =
                new FileSinkConfig(ReadonlyConfig.fromMap(writeConfig), writeRowType);
        OrcWriteStrategy writeStrategy = new OrcWriteStrategy(writeSinkConfig);

        OrcReadStrategyTest.LocalConf hadoopConf =
                new OrcReadStrategyTest.LocalConf(FS_DEFAULT_NAME_DEFAULT);
        writeStrategy.setCatalogTable(
                CatalogTableUtil.getCatalogTable("test", null, null, "test", writeRowType));
        writeStrategy.init(hadoopConf, "test1", "test1", 0);
        writeStrategy.beginTransaction(1L);
        for (int i = 0; i < ORC_WRITE_NUMBER; i++) {
            writeStrategy.write(new SeaTunnelRow(new Object[] {"test_" + i}));
        }
        writeStrategy.finishAndCloseFile();
        writeStrategy.close();

        OrcReadStrategy readStrategy = new OrcReadStrategy();
        readStrategy.init(hadoopConf);
        List<String> readFiles = readStrategy.getFileNamesByPath(TMP_PATH);
        Assertions.assertEquals(1, readFiles.size());
        String readFilePath = readFiles.get(0);

        SeaTunnelRowType readRowType = readStrategy.getSeaTunnelRowTypeInfo(readFilePath);
        Assertions.assertEquals(
                BasicType.STRING_TYPE.getSqlType(), readRowType.getFieldType(0).getSqlType());
        List<SeaTunnelRow> readRows = new ArrayList<>();
        Collector<SeaTunnelRow> readCollector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {
                        Assertions.assertTrue(record.getField(0) instanceof String);
                        readRows.add(record);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
        readStrategy.read(readFilePath, "test", readCollector);
        Assertions.assertEquals(ORC_WRITE_NUMBER, readRows.size());
        readStrategy.close();
    }
}
