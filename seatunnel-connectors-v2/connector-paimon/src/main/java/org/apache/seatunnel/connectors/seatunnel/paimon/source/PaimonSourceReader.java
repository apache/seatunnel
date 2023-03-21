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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.RowConverter;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.Table;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedDeque;

/** Paimon connector source reader. */
@Slf4j
public class PaimonSourceReader implements SourceReader<SeaTunnelRow, PaimonSourceSplit> {

    private final Deque<PaimonSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();
    private final SourceReader.Context context;
    private final Table table;
    private final SeaTunnelRowType seaTunnelRowType;
    private volatile boolean noMoreSplit;

    public PaimonSourceReader(Context context, Table table, SeaTunnelRowType seaTunnelRowType) {
        this.context = context;
        this.table = table;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() throws Exception {
        // do nothing
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            final PaimonSourceSplit split = sourceSplits.poll();
            if (Objects.nonNull(split)) {
                // read logic
                try (final RecordReader<InternalRow> reader =
                        table.newReadBuilder().newRead().createReader(split.getSplit())) {
                    final RecordReaderIterator<InternalRow> rowIterator =
                            new RecordReaderIterator<>(reader);
                    while (rowIterator.hasNext()) {
                        final InternalRow row = rowIterator.next();
                        final SeaTunnelRow seaTunnelRow =
                                RowConverter.convert(row, seaTunnelRowType);
                        output.collect(seaTunnelRow);
                    }
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded flink table store source");
                context.signalNoMoreElement();
            } else {
                log.warn("Waiting for flink table source split, sleeping 1s");
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<PaimonSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<PaimonSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
