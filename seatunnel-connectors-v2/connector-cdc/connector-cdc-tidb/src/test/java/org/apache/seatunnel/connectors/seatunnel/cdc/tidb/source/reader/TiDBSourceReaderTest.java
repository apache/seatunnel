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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;

import org.junit.jupiter.api.Test;
import org.tikv.cdc.CDCClient;
import org.tikv.kvproto.Coprocessor;

import java.lang.reflect.Field;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class TiDBSourceReaderTest {

    @Test
    void shouldAdvanceSplitWithTheSlowestRegionResolvedTimestamp() throws Exception {
        TiDBSourceConfig config =
                TiDBSourceConfig.builder().startupMode(StartupMode.LATEST).batchSize(1).build();
        TiDBSourceReader reader =
                new TiDBSourceReader(
                        mock(SourceReader.Context.class), config, mock(CatalogTable.class));
        TiDBSourceSplit split =
                new TiDBSourceSplit(
                        "database", "table", mock(Coprocessor.KeyRange.class), 10L, null, true);
        CDCClient cdcClient = mock(CDCClient.class);
        when(cdcClient.get()).thenReturn(null);
        when(cdcClient.getMinResolvedTs()).thenReturn(100L);
        when(cdcClient.getMaxResolvedTs()).thenReturn(200L);
        cdcClients(reader).put(split, cdcClient);

        reader.captureStreamingEvents(split, mock(Collector.class));

        assertEquals(100L, split.getResolvedTs());
    }

    @SuppressWarnings("unchecked")
    private Map<TiDBSourceSplit, CDCClient> cdcClients(TiDBSourceReader reader)
            throws ReflectiveOperationException {
        Field cacheField = TiDBSourceReader.class.getDeclaredField("cacheCDCClient");
        cacheField.setAccessible(true);
        return (Map<TiDBSourceSplit, CDCClient>) cacheField.get(reader);
    }
}
