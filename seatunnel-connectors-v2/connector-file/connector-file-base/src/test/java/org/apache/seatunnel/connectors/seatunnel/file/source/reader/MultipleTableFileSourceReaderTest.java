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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseMultipleTableFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.LocalFileIdentity;
import org.apache.seatunnel.connectors.seatunnel.file.source.event.FileSplitFinishedEvent;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;

class MultipleTableFileSourceReaderTest {

    @TempDir private Path tempDir;

    @Test
    void testStaleTailSplitDoesNotReadReplacementFile() throws Exception {
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
