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

package org.apache.seatunnel.connectors.cdc.base.source.reader;

import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeResolver;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.Fetcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;

class IncrementalSourceSplitReaderTest {

    @Test
    void testFetchFinishedSnapshotSplitEmitsFinishedOnlyOnce() throws Exception {
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        SourceConfig config = Mockito.mock(SourceConfig.class);
        SchemaChangeResolver resolver = Mockito.mock(SchemaChangeResolver.class);

        IncrementalSourceSplitReader<SourceConfig> reader =
                new IncrementalSourceSplitReader<SourceConfig>(0, dialect, config, resolver) {
                    @Override
                    protected void checkSplitOrStartNext() {}
                };

        @SuppressWarnings("unchecked")
        Fetcher<SourceRecords, SourceSplitBase> fetcher = Mockito.mock(Fetcher.class);
        Mockito.when(fetcher.pollSplitRecords()).thenReturn(null);

        setField(reader, "currentFetcher", fetcher);
        setField(reader, "currentSplitId", "split-1");

        RecordsWithSplitIds<SourceRecords> first = reader.fetch();
        RecordsWithSplitIds<SourceRecords> second = reader.fetch();

        Assertions.assertEquals(Collections.singleton("split-1"), first.finishedSplits());
        Assertions.assertFalse(first.finishedSplits().contains(null));
        Assertions.assertEquals(Collections.emptySet(), second.finishedSplits());
        Assertions.assertFalse(second.finishedSplits().contains(null));
        Mockito.verify(fetcher, Mockito.times(1)).pollSplitRecords();
    }

    @Test
    void testFetchFinishedSnapshotSplitFailFastWhenCurrentSplitIdIsNull() throws Exception {
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        SourceConfig config = Mockito.mock(SourceConfig.class);
        SchemaChangeResolver resolver = Mockito.mock(SchemaChangeResolver.class);

        IncrementalSourceSplitReader<SourceConfig> reader =
                new IncrementalSourceSplitReader<SourceConfig>(0, dialect, config, resolver) {
                    @Override
                    protected void checkSplitOrStartNext() {}
                };

        @SuppressWarnings("unchecked")
        Fetcher<SourceRecords, SourceSplitBase> fetcher = Mockito.mock(Fetcher.class);
        Mockito.when(fetcher.pollSplitRecords()).thenReturn(null);

        setField(reader, "currentFetcher", fetcher);
        setField(reader, "currentSplitId", null);

        Assertions.assertThrows(IOException.class, reader::fetch);
    }

    @Test
    void testFetchFinishedSnapshotSplitSupportsNextSplitAfterIdChanges() throws Exception {
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        SourceConfig config = Mockito.mock(SourceConfig.class);
        SchemaChangeResolver resolver = Mockito.mock(SchemaChangeResolver.class);

        IncrementalSourceSplitReader<SourceConfig> reader =
                new IncrementalSourceSplitReader<SourceConfig>(0, dialect, config, resolver) {
                    @Override
                    protected void checkSplitOrStartNext() {}
                };

        @SuppressWarnings("unchecked")
        Fetcher<SourceRecords, SourceSplitBase> fetcher = Mockito.mock(Fetcher.class);
        Mockito.when(fetcher.pollSplitRecords()).thenReturn(null);

        setField(reader, "currentFetcher", fetcher);
        setField(reader, "currentSplitId", "split-1");

        RecordsWithSplitIds<SourceRecords> first = reader.fetch();
        RecordsWithSplitIds<SourceRecords> idle = reader.fetch();

        setField(reader, "currentSplitId", "split-2");
        RecordsWithSplitIds<SourceRecords> second = reader.fetch();

        Assertions.assertEquals(Collections.singleton("split-1"), first.finishedSplits());
        Assertions.assertEquals(Collections.emptySet(), idle.finishedSplits());
        Assertions.assertEquals(Collections.singleton("split-2"), second.finishedSplits());
        Mockito.verify(fetcher, Mockito.times(2)).pollSplitRecords();
    }

    @Test
    void testCloseClearsState() throws Exception {
        DataSourceDialect<SourceConfig> dialect = Mockito.mock(DataSourceDialect.class);
        SourceConfig config = Mockito.mock(SourceConfig.class);
        SchemaChangeResolver resolver = Mockito.mock(SchemaChangeResolver.class);

        IncrementalSourceSplitReader<SourceConfig> reader =
                new IncrementalSourceSplitReader<SourceConfig>(0, dialect, config, resolver) {
                    @Override
                    protected void checkSplitOrStartNext() {}
                };

        @SuppressWarnings("unchecked")
        Fetcher<SourceRecords, SourceSplitBase> fetcher = Mockito.mock(Fetcher.class);

        setField(reader, "currentFetcher", fetcher);
        setField(reader, "currentSplitId", "split-1");
        setField(reader, "emittedFinishedSplitId", "split-1");

        reader.close();

        Assertions.assertNull(getField(reader, "currentSplitId"));
        Assertions.assertNull(getField(reader, "emittedFinishedSplitId"));
        Mockito.verify(fetcher, Mockito.times(1)).close();
    }

    private static void setField(
            IncrementalSourceSplitReader<?> reader, String fieldName, Object value)
            throws Exception {
        Field field = IncrementalSourceSplitReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(reader, value);
    }

    private static Object getField(IncrementalSourceSplitReader<?> reader, String fieldName)
            throws Exception {
        Field field = IncrementalSourceSplitReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(reader);
    }
}
