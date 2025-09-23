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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.SeaTunnelRowDeserializer;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

@Slf4j
public abstract class IoTDBv2AbstractSourceReader
        implements SourceReader<SeaTunnelRow, IoTDBv2SourceSplit> {

    protected final ReadonlyConfig conf;

    private final Queue<IoTDBv2SourceSplit> pendingSplits;

    private final SourceReader.Context context;

    protected SeaTunnelRowDeserializer deserializer;

    private volatile boolean noMoreSplitsAssignment;

    public IoTDBv2AbstractSourceReader(ReadonlyConfig conf, SourceReader.Context readerContext) {
        this.conf = conf;
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                IoTDBv2SourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }
        if (Boundedness.BOUNDED.equals(context.getBoundedness())
                && noMoreSplitsAssignment
                && pendingSplits.isEmpty()) {
            log.info("Closed the bounded iotdb source");
            context.signalNoMoreElement();
        }
    }

    public abstract void read(IoTDBv2SourceSplit split, Collector<SeaTunnelRow> output)
            throws Exception;

    @Override
    public List<IoTDBv2SourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<IoTDBv2SourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // do nothing
    }
}
