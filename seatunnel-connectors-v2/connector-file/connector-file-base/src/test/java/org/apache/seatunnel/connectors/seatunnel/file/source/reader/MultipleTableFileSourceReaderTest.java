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
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileFormat;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.LocalFileIdentity;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class MultipleTableFileSourceReaderTest {

    @TempDir private Path tempDir;

    @Test
    void testStaleTailSplitDoesNotReadReplacementFile() throws Exception {
        assumeStableFileIdentity();
        Path file = tempDir.resolve("application.log");
        Files.write(file, "old\n".getBytes());
        String originalIdentity = LocalFileIdentity.read(file.toString());
        replaceFile(file, "replacement\n");

        ReaderFixture fixture = createReader();
        FileSourceSplit split =
                new FileSourceSplit(
                        fixture.tableId, file.toString(), 0L, 4L, originalIdentity, null);
        fixture.reader.addSplits(Collections.singletonList(split));
        fixture.reader.pollNext(fixture.collector);

        Mockito.verify(fixture.readStrategy, Mockito.never())
                .read(Mockito.any(FileSourceSplit.class), Mockito.any());
        Assertions.assertEquals(0L, captureFinishedEvent(fixture.context).getProcessedBytes());
    }

    @Test
    void testMissingTailFileDoesNotFailReader() throws Exception {
        assumeStableFileIdentity();
        Path file = tempDir.resolve("application.log");
        Files.write(file, "old\n".getBytes());
        String originalIdentity = LocalFileIdentity.read(file.toString());
        Files.delete(file);

        ReaderFixture fixture = createReader();
        FileSourceSplit split =
                new FileSourceSplit(
                        fixture.tableId, file.toString(), 0L, 4L, originalIdentity, null);
        fixture.reader.addSplits(Collections.singletonList(split));
        fixture.reader.pollNext(fixture.collector);

        Mockito.verify(fixture.readStrategy, Mockito.never())
                .read(Mockito.any(FileSourceSplit.class), Mockito.any());
        Assertions.assertEquals(0L, captureFinishedEvent(fixture.context).getProcessedBytes());
    }

    @Test
    void testFileReplacementDuringReadFailsBeforeAcknowledgement() throws Exception {
        assumeStableFileIdentity();
        Path file = tempDir.resolve("application.log");
        Files.write(file, "old\n".getBytes());
        String originalIdentity = LocalFileIdentity.read(file.toString());

        ReaderFixture fixture = createReader();
        Mockito.doAnswer(
                        invocation -> {
                            replaceFile(file, "replacement\n");
                            return null;
                        })
                .when(fixture.readStrategy)
                .read(Mockito.any(FileSourceSplit.class), Mockito.any());
        FileSourceSplit split =
                new FileSourceSplit(
                        fixture.tableId, file.toString(), 0L, 4L, originalIdentity, null);
        fixture.reader.addSplits(Collections.singletonList(split));

        Assertions.assertThrows(
                FileConnectorException.class, () -> fixture.reader.pollNext(fixture.collector));
        Mockito.verify(fixture.context, Mockito.never()).sendSourceEventToEnumerator(Mockito.any());
    }

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

    private ReaderFixture createReader() {
        String tableId =
                TableIdentifier.of("catalog", "database", "table").toTablePath().toString();
        ReadStrategy readStrategy = Mockito.mock(ReadStrategy.class);
        CatalogTable catalogTable = Mockito.mock(CatalogTable.class);
        Mockito.when(catalogTable.getTableId())
                .thenReturn(TableIdentifier.of("catalog", "database", "table"));

        BaseFileSourceConfig fileSourceConfig = Mockito.mock(BaseFileSourceConfig.class);
        Mockito.when(fileSourceConfig.getCatalogTable()).thenReturn(catalogTable);
        Mockito.when(fileSourceConfig.getReadStrategy()).thenReturn(readStrategy);

        BaseMultipleTableFileSourceConfig multipleTableConfig =
                Mockito.mock(BaseMultipleTableFileSourceConfig.class);
        Mockito.when(multipleTableConfig.getFileSourceConfigs())
                .thenReturn(Collections.singletonList(fileSourceConfig));

        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        @SuppressWarnings("unchecked")
        Collector<SeaTunnelRow> collector = Mockito.mock(Collector.class);
        Mockito.when(collector.getCheckpointLock()).thenReturn(new Object());

        return new ReaderFixture(
                tableId,
                readStrategy,
                context,
                collector,
                new MultipleTableFileSourceReader(context, multipleTableConfig));
    }

    private void assumeStableFileIdentity() {
        try {
            LocalFileIdentity.read(tempDir.toString());
        } catch (Exception e) {
            Assumptions.assumeTrue(false, "The filesystem does not expose a stable file key");
        }
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

    private static void replaceFile(Path file, String content) throws Exception {
        Path replacement = file.resolveSibling(file.getFileName() + ".replacement");
        Files.write(replacement, content.getBytes());
        Files.move(replacement, file, StandardCopyOption.REPLACE_EXISTING);
    }

    private static FileSplitFinishedEvent captureFinishedEvent(SourceReader.Context context) {
        ArgumentCaptor<SourceEvent> eventCaptor = ArgumentCaptor.forClass(SourceEvent.class);
        Mockito.verify(context).sendSourceEventToEnumerator(eventCaptor.capture());
        return (FileSplitFinishedEvent) eventCaptor.getValue();
    }

    private static final class ReaderFixture {
        private final String tableId;
        private final ReadStrategy readStrategy;
        private final SourceReader.Context context;
        private final Collector<SeaTunnelRow> collector;
        private final MultipleTableFileSourceReader reader;

        private ReaderFixture(
                String tableId,
                ReadStrategy readStrategy,
                SourceReader.Context context,
                Collector<SeaTunnelRow> collector,
                MultipleTableFileSourceReader reader) {
            this.tableId = tableId;
            this.readStrategy = readStrategy;
            this.context = context;
            this.collector = collector;
            this.reader = reader;
        }
    }
}
