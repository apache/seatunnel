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

package org.apache.seatunnel.connectors.seatunnel.kudu.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduInputFormat;

import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;

@Slf4j
public class KuduSourceReader implements SourceReader<SeaTunnelRow, KuduSourceSplit> {

    private final SourceReader.Context context;

    private final KuduInputFormat kuduInputFormat;
    Deque<KuduSourceSplit> splits = new LinkedList<>();

    boolean noMoreSplit;

    public KuduSourceReader(KuduInputFormat kuduInputFormat, SourceReader.Context context) {
        this.context = context;
        this.kuduInputFormat = kuduInputFormat;
    }

    @Override
    public void open() {
        kuduInputFormat.openInputFormat();
    }

    @Override
    public void close() {
        kuduInputFormat.closeInputFormat();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            KuduSourceSplit split = splits.poll();
            if (null != split) {
                KuduScanner kuduScanner = kuduInputFormat.scanner(split.getToken());
                while (kuduScanner.hasMoreRows()) {
                    RowResultIterator rowResults = kuduScanner.nextRows();
                    while (rowResults.hasNext()) {
                        RowResult rowResult = rowResults.next();
                        SeaTunnelRow seaTunnelRow = kuduInputFormat.toInternal(rowResult);
                        output.collect(seaTunnelRow);
                    }
                }
            } else if (noMoreSplit && splits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded kudu source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @Override
    public List<KuduSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(splits);
    }

    @Override
    public void addSplits(List<KuduSourceSplit> splits) {
        this.splits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
