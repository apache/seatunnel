/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hbase.source;

import org.apache.seatunnel.shade.com.google.common.base.Preconditions;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;
import org.apache.seatunnel.connectors.seatunnel.hbase.format.HBaseDeserializationFormat;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

@Slf4j
public class HbaseSourceReader implements SourceReader<SeaTunnelRow, HbaseSourceSplit> {
    private static final String ROW_KEY = "rowkey";
    private final Deque<HbaseSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();

    private final transient Map<String, byte[][]> namesMap;

    private final Context context;
    private final SeaTunnelRowType seaTunnelRowType;
    private volatile boolean noMoreSplit = false;
    private final HbaseClient hbaseClient;

    private HbaseParameters hbaseParameters;
    private final List<String> columnNames;

    private HBaseDeserializationFormat hbaseDeserializationFormat =
            new HBaseDeserializationFormat();
    private ResultScanner currentScanner;

    public HbaseSourceReader(
            HbaseParameters hbaseParameters, Context context, SeaTunnelRowType seaTunnelRowType) {
        this.hbaseParameters = hbaseParameters;
        this.context = context;
        this.seaTunnelRowType = seaTunnelRowType;
        this.namesMap = Maps.newConcurrentMap();

        this.columnNames =
                Arrays.asList(seaTunnelRowType.getFieldNames()).stream()
                        .filter(name -> !ROW_KEY.equals(name))
                        .collect(Collectors.toList());
        // Check if input column names are in format: [ columnFamily:column ].
        this.columnNames.stream()
                .forEach(
                        column ->
                                Preconditions.checkArgument(
                                        column.contains(":") && column.split(":").length == 2,
                                        "Invalid column names, it should be [ColumnFamily:Column] format"));
        hbaseClient = HbaseClient.createInstance(hbaseParameters);
    }

    @Override
    public void open() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        if (this.currentScanner != null) {
            try {
                this.currentScanner.close();
            } catch (Exception e) {
                throw new IOException("Failed to close HBase Scanner.", e);
            }
        }
        if (this.hbaseClient != null) {
            try {
                this.hbaseClient.close();
            } catch (Exception e) {
                throw new IOException("Failed to close HBase connection.", e);
            }
            log.info("Current HBase connection is closed.");
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            final HbaseSourceSplit split = sourceSplits.poll();
            if (Objects.nonNull(split)) {
                // read logic
                if (currentScanner == null) {
                    currentScanner = hbaseClient.scan(split, hbaseParameters, this.columnNames);
                }
                for (Result result : currentScanner) {
                    SeaTunnelRow seaTunnelRow =
                            hbaseDeserializationFormat.deserialize(
                                    convertRawRow(result), seaTunnelRowType);
                    output.collect(seaTunnelRow);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded Hbase source");
                context.signalNoMoreElement();
            } else {
                log.warn("Waiting for Hbase split, sleeping 1s");
                Thread.sleep(1000L);
            }
        }
    }

    private byte[][] convertRawRow(Result result) {
        String[] fieldNames = seaTunnelRowType.getFieldNames();
        byte[][] rawRow = new byte[fieldNames.length][];
        for (int i = 0; i < fieldNames.length; ++i) {
            String columnName = fieldNames[i];
            byte[] bytes;
            try {
                // handle rowkey column
                if (ROW_KEY.equals(columnName)) {
                    bytes = result.getRow();
                } else {
                    byte[][] arr = this.namesMap.get(columnName);
                    // Deduplicate
                    if (Objects.isNull(arr)) {
                        arr = new byte[2][];
                        String[] arr1 = columnName.split(":");
                        arr[0] = arr1[0].trim().getBytes(StandardCharsets.UTF_8);
                        arr[1] = arr1[1].trim().getBytes(StandardCharsets.UTF_8);
                        this.namesMap.put(columnName, arr);
                    }
                    bytes = result.getValue(arr[0], arr[1]);
                }
                rawRow[i] = bytes;
            } catch (Exception e) {
                log.error(
                        "Cannot read data from {}, reason: \n", this.hbaseParameters.getTable(), e);
            }
        }
        return rawRow;
    }

    @Override
    public List<HbaseSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<HbaseSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
