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

package org.apache.seatunnel.connectors.seatunnel.file.sink.writer;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.fs.Path;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.CompressFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.sink.config.FileSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.SourceFileNameCollector;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class PreserveSourceFilenameTest {

    @Test
    void routesRowsFromMultipleSourceFilesToTheirOriginalNames() {
        TestWriteStrategy strategy = newStrategy();

        String ordersPath = strategy.getOrCreateFilePathBeingWritten(row("orders.csv", "file-1"));
        String customersPath =
                strategy.getOrCreateFilePathBeingWritten(row("customers.csv", "file-2"));
        String secondOrdersPath =
                strategy.getOrCreateFilePathBeingWritten(row("orders.csv", "file-1"));

        Assertions.assertEquals("orders.csv", new Path(ordersPath).getName());
        Assertions.assertEquals("customers.csv", new Path(customersPath).getName());
        Assertions.assertNotEquals(ordersPath, customersPath);
        Assertions.assertEquals(ordersPath, secondOrdersPath);
    }

    @Test
    void rejectsMissingSourceFileMetadata() {
        TestWriteStrategy strategy = newStrategy();

        Assertions.assertThrows(
                FileConnectorException.class,
                () -> strategy.getOrCreateFilePathBeingWritten(new SeaTunnelRow(new Object[] {1})));
    }

    @Test
    void rejectsDuplicateNamesFromDifferentSourceFiles() {
        TestWriteStrategy strategy = newStrategy();
        strategy.getOrCreateFilePathBeingWritten(row("orders.csv", "file-1"));

        Assertions.assertThrows(
                FileConnectorException.class,
                () -> strategy.getOrCreateFilePathBeingWritten(row("orders.csv", "another-file")));
    }

    @Test
    void rejectsConflictingFilenameOptions() {
        Map<String, Object> values = new HashMap<>();
        values.put("path", "/output");
        values.put("preserve_source_filename", true);
        values.put("custom_filename", true);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"id"}, new SeaTunnelDataType[] {BasicType.INT_TYPE});

        Assertions.assertThrows(
                FileConnectorException.class,
                () -> new FileSinkConfig(ReadonlyConfig.fromMap(values), rowType));
    }

    private static SeaTunnelRow row(String fileName, String fileId) {
        SeaTunnelRow row = new SeaTunnelRow(new Object[] {1});
        row.getOptions().put(SourceFileNameCollector.SOURCE_FILE_NAME, fileName);
        row.getOptions().put(SourceFileNameCollector.SOURCE_FILE_ID, fileId);
        return row;
    }

    private static TestWriteStrategy newStrategy() {
        FileSinkConfig config = Mockito.mock(FileSinkConfig.class);
        Mockito.when(config.getSinkColumnsIndexInRow()).thenReturn(Collections.emptyList());
        Mockito.when(config.getPartitionFieldsIndexInRow()).thenReturn(Collections.emptyList());
        Mockito.when(config.getBatchSize()).thenReturn(1000);
        Mockito.when(config.getCompressFormat()).thenReturn(CompressFormat.NONE);
        Mockito.when(config.isSingleFileMode()).thenReturn(false);
        Mockito.when(config.isPreserveSourceFilename()).thenReturn(true);
        Mockito.when(config.getTmpPath()).thenReturn("/tmp/seatunnel");

        TestWriteStrategy strategy = new TestWriteStrategy(config);
        strategy.startTransaction();
        return strategy;
    }

    private static class TestWriteStrategy extends AbstractWriteStrategy<Object> {

        private TestWriteStrategy(FileSinkConfig fileSinkConfig) {
            super(fileSinkConfig);
        }

        private void startTransaction() {
            this.jobId = "job-1";
            this.uuidPrefix = "writer-1";
            this.subTaskIndex = 0;
            beginTransaction(1L);
        }

        @Override
        public Object getOrCreateOutputStream(String path) {
            return new Object();
        }

        @Override
        public void finishAndCloseFile() {}
    }
}
