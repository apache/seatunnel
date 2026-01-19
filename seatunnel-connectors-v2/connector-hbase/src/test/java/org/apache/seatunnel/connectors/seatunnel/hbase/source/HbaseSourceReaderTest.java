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

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HbaseSourceReaderTest {

    private static class CountingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();
        private int count;

        @Override
        public void collect(SeaTunnelRow record) {
            count++;
        }

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }

        public int getCount() {
            return count;
        }
    }

    @Test
    void testPollNextReadsAllSplits() throws Exception {
        HbaseParameters hbaseParameters = mock(HbaseParameters.class);
        when(hbaseParameters.getTable()).thenReturn("test_table");

        SourceReader.Context readerContext = mock(SourceReader.Context.class);
        HbaseClient hbaseClient = mock(HbaseClient.class);

        SeaTunnelRowType seaTunnelRowType =
                new SeaTunnelRowType(
                        new String[] {"rowkey", "cf1:id", "cf1:name"},
                        new SeaTunnelDataType[] {
                            BasicType.STRING_TYPE, BasicType.STRING_TYPE, BasicType.STRING_TYPE
                        });

        HbaseSourceReader reader =
                new HbaseSourceReader(
                        hbaseParameters, readerContext, seaTunnelRowType, hbaseClient);

        HbaseSourceSplit split0 = new HbaseSourceSplit(0, Bytes.toBytes("a"), Bytes.toBytes("b"));
        HbaseSourceSplit split1 = new HbaseSourceSplit(1, Bytes.toBytes("b"), Bytes.toBytes("c"));

        Result result0 = mock(Result.class);
        when(result0.getRow()).thenReturn(Bytes.toBytes("row0"));
        when(result0.getValue(any(byte[].class), any(byte[].class)))
                .thenReturn(Bytes.toBytes("v0"));

        Result result1 = mock(Result.class);
        when(result1.getRow()).thenReturn(Bytes.toBytes("row1"));
        when(result1.getValue(any(byte[].class), any(byte[].class)))
                .thenReturn(Bytes.toBytes("v1"));

        ResultScanner scanner0 = mock(ResultScanner.class);
        when(scanner0.iterator()).thenReturn(Arrays.asList(result0).iterator());
        ResultScanner scanner1 = mock(ResultScanner.class);
        when(scanner1.iterator()).thenReturn(Arrays.asList(result1).iterator());

        when(hbaseClient.scan(eq(split0), eq(hbaseParameters), anyList())).thenReturn(scanner0);
        when(hbaseClient.scan(eq(split1), eq(hbaseParameters), anyList())).thenReturn(scanner1);

        reader.addSplits(Arrays.asList(split0, split1));
        reader.handleNoMoreSplits();

        CountingCollector collector = new CountingCollector();
        reader.pollNext(collector);
        reader.pollNext(collector);
        reader.pollNext(collector);

        assertEquals(2, collector.getCount());
        verify(hbaseClient, times(1)).scan(eq(split0), eq(hbaseParameters), anyList());
        verify(hbaseClient, times(1)).scan(eq(split1), eq(hbaseParameters), anyList());
        verify(scanner0, times(1)).close();
        verify(scanner1, times(1)).close();
        verify(readerContext, times(1)).signalNoMoreElement();
    }
}
