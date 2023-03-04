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

package org.apache.seatunnel.connectors.seatunnel.starrocks.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.starrocks.client.source.StarRocksBeReadClient;
import org.apache.seatunnel.connectors.seatunnel.starrocks.config.SourceConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Slf4j
public class StarRocksSourceReader implements SourceReader<SeaTunnelRow, StarRocksSourceSplit> {

    private final Queue<StarRocksSourceSplit> pendingSplits;
    private final SourceReader.Context context;
    private final SourceConfig sourceConfig;
    private final SeaTunnelRowType seaTunnelRowType;
    private volatile boolean noMoreSplitsAssignment;

    public StarRocksSourceReader(
            SourceReader.Context readerContext,
            SeaTunnelRowType seaTunnelRowType,
            SourceConfig sourceConfig) {
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
        this.sourceConfig = sourceConfig;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                StarRocksSourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())
                && noMoreSplitsAssignment
                && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded StarRocks source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<StarRocksSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<StarRocksSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    private void read(StarRocksSourceSplit split, Collector<SeaTunnelRow> output) {
        StarRocksBeReadClient client =
                new StarRocksBeReadClient(split.getPartition(), sourceConfig, seaTunnelRowType);
        // open scanner to be
        client.openScanner();
        while (client.hasNext()) {
            SeaTunnelRow seaTunnelRow = client.getNext();
            output.collect(seaTunnelRow);
        }
        // close client to be
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {
        // nothing to do
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
