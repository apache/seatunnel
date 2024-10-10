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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer.SeaTunnelRowSnapshotRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.deserializer.SeaTunnelRowStreamingRecordDeserializer;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.utils.TableKeyRangeUtils;

import org.tikv.cdc.CDCClient;
import org.tikv.common.TiSession;
import org.tikv.common.key.RowKey;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Coprocessor;
import org.tikv.kvproto.Kvrpcpb;
import org.tikv.shade.com.google.protobuf.ByteString;
import org.tikv.txn.KVClient;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class TiDBSourceReader implements SourceReader<SeaTunnelRow, TiDBSourceSplit> {

    private final SourceReader.Context context;
    private final TiDBSourceConfig config;
    private final List<TiDBSourceSplit> sourceSplits;

    private final Map<TiDBSourceSplit, CDCClient> cacheCDCClient;

    private SeaTunnelRowSnapshotRecordDeserializer snapshotRecordDeserializer;
    private SeaTunnelRowStreamingRecordDeserializer streamingRecordDeserializer;

    private transient TiSession session;

    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> preWrites;
    private transient TreeMap<RowKeyWithTs, Cdcpb.Event.Row> commits;
    private transient BlockingQueue<Cdcpb.Event.Row> committedEvents;

    private CatalogTable catalogTable;

    public TiDBSourceReader(Context context, TiDBSourceConfig config, CatalogTable catalogTable) {
        this.context = context;
        this.config = config;
        this.sourceSplits = new ArrayList<>();

        this.cacheCDCClient = new HashMap<>();

        this.preWrites = new TreeMap<>();
        this.commits = new TreeMap<>();
        // cdc event will lose if pull cdc event block when region split
        // use queue to separate read and write to ensure pull event unblock.
        // since sink jdbc is slow, 5000W queue size may be safe size.
        this.committedEvents = new LinkedBlockingQueue<>();
        this.catalogTable = catalogTable;
    }

    /** Open the source reader. */
    @Override
    public void open() throws Exception {
        this.session = TiSession.create(config.getTiConfiguration());
        TiTableInfo tableInfo =
                session.getCatalog().getTable(config.getDatabaseName(), config.getTableName());
        this.snapshotRecordDeserializer =
                new SeaTunnelRowSnapshotRecordDeserializer(tableInfo, catalogTable);
        this.streamingRecordDeserializer =
                new SeaTunnelRowStreamingRecordDeserializer(tableInfo, catalogTable);
    }

    /**
     * Called to close the reader, in case it holds on to any resources, like threads or network
     * connections.
     */
    @Override
    public void close() throws IOException {
        if (this.session != null) {
            try {
                this.session.close();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Generate the next batch of records.
     *
     * @param output output collector.
     * @throws Exception if error occurs.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (config.getStartupMode() == StartupMode.INITIAL) {
            for (TiDBSourceSplit sourceSplit : sourceSplits) {
                if (!sourceSplit.isSnapshotCompleted()) {
                    snapshotEvents(sourceSplit, output);
                    sourceSplit.setSnapshotCompleted(true);
                }
            }
        }
        Iterator<TiDBSourceSplit> iterator = sourceSplits.iterator();
        while (iterator.hasNext()) {
            TiDBSourceSplit sourceSplit = iterator.next();
            captureStreamingEvents(sourceSplit, output);
        }
    }

    protected void snapshotEvents(TiDBSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        log.info(String.format("[%s] Snapshot events start.", split.splitId()));
        Coprocessor.KeyRange keyRange = split.getKeyRange();
        try (KVClient scanClient = session.createKVClient()) {
            // start timestamp
            long startTs = session.getTimestamp().getVersion();
            ByteString start = split.getSnapshotStart();
            while (true) {
                final List<Kvrpcpb.KvPair> segment =
                        scanClient.scan(start, keyRange.getEnd(), startTs);
                if (segment.isEmpty()) {
                    split.setResolvedTs(startTs);
                    break;
                }
                for (Kvrpcpb.KvPair record : segment) {
                    if (TableKeyRangeUtils.isRecordKey(record.getKey().toByteArray())) {
                        snapshotRecordDeserializer.deserialize(record, output);
                    }
                }
                start =
                        RowKey.toRawKey(segment.get(segment.size() - 1).getKey())
                                .next()
                                .toByteString();
                // set snapshot offset
                split.setSnapshotStart(start);
            }
        }
    }

    protected void captureStreamingEvents(TiDBSourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception {
        long resolvedTs = split.getResolvedTs();
        log.info("Capture streaming event from resolvedTs:{}", resolvedTs);
        CDCClient cdcClient = getCdcClient(split, resolvedTs);
        for (int i = 0; i < config.getBatchSize(); i++) {
            final Cdcpb.Event.Row row = cdcClient.get();
            if (row == null) {
                break;
            }
            handleRow(row);
        }
        resolvedTs = cdcClient.getMaxResolvedTs();
        if (commits.size() > 0) {
            flushRows(resolvedTs);
        }
        // ouput data
        while (!committedEvents.isEmpty()) {
            Cdcpb.Event.Row row = committedEvents.take();
            this.streamingRecordDeserializer.deserialize(row, output);
        }
        // reset resolvedTs
        log.info("Capture streaming event next resolvedTs:{}", resolvedTs);
        split.setResolvedTs(resolvedTs);
    }

    private CDCClient getCdcClient(TiDBSourceSplit split, long finalResolvedTs) {
        CDCClient cdcClient =
                cacheCDCClient.computeIfAbsent(
                        split,
                        k -> {
                            CDCClient client = new CDCClient(session, k.getKeyRange());
                            client.start(finalResolvedTs);
                            return client;
                        });
        return cdcClient;
    }

    /**
     * Get the current split checkpoint state by checkpointId.
     *
     * <p>If the source is bounded, checkpoint is not triggered.
     *
     * @param checkpointId checkpoint Id.
     * @return split checkpoint state.
     * @throws Exception if error occurs.
     */
    @Override
    public List<TiDBSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    /**
     * Add the split checkpoint state to reader.
     *
     * @param splits split checkpoint state.
     */
    @Override
    public void addSplits(List<TiDBSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    /**
     * This method is called when the reader is notified that it will not receive any further
     * splits.
     *
     * <p>It is triggered when the enumerator calls {@link
     * SourceSplitEnumerator.Context#signalNoMoreSplits(int)} with the reader's parallel subtask.
     */
    @Override
    public void handleNoMoreSplits() {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}

    private void handleRow(final Cdcpb.Event.Row row) {
        if (!TableKeyRangeUtils.isRecordKey(row.getKey().toByteArray())) {
            // Don't handle index key for now
            return;
        }
        log.debug("binlog record, type: {}, data: {}", row.getType(), row);
        switch (row.getType()) {
            case COMMITTED:
                preWrites.put(RowKeyWithTs.ofStart(row), row);
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case COMMIT:
                commits.put(RowKeyWithTs.ofCommit(row), row);
                break;
            case PREWRITE:
                preWrites.put(RowKeyWithTs.ofStart(row), row);
                break;
            case ROLLBACK:
                preWrites.remove(RowKeyWithTs.ofStart(row));
                break;
            default:
                log.warn("Unsupported row type:" + row.getType());
        }
    }

    protected void flushRows(final long resolvedTs) throws Exception {
        while (!commits.isEmpty() && commits.firstKey().getTimestamp() <= resolvedTs) {
            final Cdcpb.Event.Row commitRow = commits.pollFirstEntry().getValue();
            final Cdcpb.Event.Row prewriteRow = preWrites.remove(RowKeyWithTs.ofStart(commitRow));
            // if pull cdc event block when region split, cdc event will lose.
            committedEvents.offer(prewriteRow);
        }
    }
}
