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

package org.apache.seatunnel.connectors.seatunnel.file.source.reader;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class MultipleTableFileSourceReaderTest {

    @Test
    void shouldPreserveNonMarkdownFailureContext() throws Exception {
        String source = "topic:v1";
        IOException failure = new IOException("read failed");
        ReadStrategy readStrategy = Mockito.mock(ReadStrategy.class);
        MultipleTableFileSourceReader reader =
                createReader("json_table", FileFormat.JSON, false, readStrategy);
        String tableId = tableId("json_table");
        FileSourceSplit split = new FileSourceSplit(tableId, source);
        Collector<SeaTunnelRow> output = collector();
        Mockito.doThrow(failure).when(readStrategy).read(split, output);
        reader.addSplits(Collections.singletonList(split));

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class, () -> reader.pollNext(output));

        Assertions.assertTrue(exception.getMessage().contains(split.splitId()));
        Assertions.assertSame(failure, exception.getCause());
    }

    @Test
    void shouldSanitizeMarkdownFailureContextAndRetainStackTrace() throws Exception {
        String source = "https://user:secret@example.com/docs/a.md" + "?X-Amz-Signature=value#part";
        IOException failure = new IOException("read failed for " + source);
        failure.setStackTrace(
                new StackTraceElement[] {
                    new StackTraceElement("RemoteReader", "read", "RemoteReader.java", 42)
                });
        ReadStrategy readStrategy = Mockito.mock(ReadStrategy.class);
        MultipleTableFileSourceReader reader =
                createReader("markdown_table", FileFormat.MARKDOWN, true, readStrategy);
        FileSourceSplit split = new FileSourceSplit(tableId("markdown_table"), source);
        Collector<SeaTunnelRow> output = collector();
        Mockito.doThrow(failure).when(readStrategy).read(split, output);
        reader.addSplits(Collections.singletonList(split));

        FileConnectorException exception =
                Assertions.assertThrows(
                        FileConnectorException.class, () -> reader.pollNext(output));

        Assertions.assertTrue(exception.getMessage().contains("https://example.com/docs/a.md"));
        Assertions.assertNotNull(exception.getCause());
        Assertions.assertNotSame(failure, exception.getCause());
        Assertions.assertArrayEquals(failure.getStackTrace(), exception.getCause().getStackTrace());
        Assertions.assertTrue(
                exception.getCause().getMessage().contains(IOException.class.getName()));
        assertDoesNotExposeSensitiveSource(exception);
    }

    private static MultipleTableFileSourceReader createReader(
            String tableName,
            FileFormat fileFormat,
            boolean metadataEnabled,
            ReadStrategy strategy) {
        CatalogTable catalogTable = catalogTable(tableName);
        Map<String, Object> values = new HashMap<>();
        values.put(FileBaseSourceOptions.MARKDOWN_RAG_METADATA_ENABLED.key(), metadataEnabled);

        BaseFileSourceConfig fileConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(fileConfig.getCatalogTable()).thenReturn(catalogTable);
        Mockito.when(fileConfig.getReadStrategy()).thenReturn(strategy);
        Mockito.when(fileConfig.getFileFormat()).thenReturn(fileFormat);
        Mockito.when(fileConfig.getBaseFileSourceConfig())
                .thenReturn(ReadonlyConfig.fromMap(values));

        BaseMultipleTableFileSourceConfig multipleConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(fileConfig));
        return new MultipleTableFileSourceReader(
                Mockito.mock(SourceReader.Context.class), multipleConfig);
    }

    private static String tableId(String tableName) {
        return catalogTable(tableName).getTableId().toTablePath().toString();
    }

    private static CatalogTable catalogTable(String tableName) {
        return CatalogTableUtil.getCatalogTable(
                tableName,
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE}));
    }

    @SuppressWarnings("unchecked")
    private static Collector<SeaTunnelRow> collector() {
        Collector<SeaTunnelRow> output = Mockito.mock(Collector.class);
        Mockito.when(output.getCheckpointLock()).thenReturn(new Object());
        return output;
    }

    private static void assertDoesNotExposeSensitiveSource(Throwable throwable) {
        StringBuilder rendered = new StringBuilder();
        for (Throwable current = throwable; current != null; current = current.getCause()) {
            rendered.append(current.getMessage()).append('\n');
        }
        String message = rendered.toString();
        Assertions.assertFalse(message.contains("user"));
        Assertions.assertFalse(message.contains("secret"));
        Assertions.assertFalse(message.contains("X-Amz-Signature"));
        Assertions.assertFalse(message.contains("value"));
        Assertions.assertFalse(message.contains("part"));
    }
}
