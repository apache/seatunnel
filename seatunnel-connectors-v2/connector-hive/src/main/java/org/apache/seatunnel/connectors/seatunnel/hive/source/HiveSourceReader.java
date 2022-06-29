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

package org.apache.seatunnel.connectors.seatunnel.hive.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.format.ReadStrategy;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class HiveSourceReader implements SourceReader<SeaTunnelRow, HiveSourceSplit> {

    private static final long THREAD_WAIT_TIME = 500L;

    private ReadStrategy readStrategy;

    private HadoopConf hadoopConf;

    private Set<HiveSourceSplit> sourceSplits;

    private final SourceReader.Context context;

    public HiveSourceReader(ReadStrategy readStrategy, HadoopConf hadoopConf, SourceReader.Context context) {
        this.readStrategy = readStrategy;
        this.hadoopConf = hadoopConf;
        this.context = context;
        this.sourceSplits = new HashSet<>();
    }

    @Override
    public void open() {
        readStrategy.init(hadoopConf);
    }

    @Override
    public void close() {

    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (sourceSplits.isEmpty()) {
            Thread.sleep(THREAD_WAIT_TIME);
            return;
        }
        sourceSplits.forEach(source -> {
            try {
                readStrategy.read(source.splitId(), output);
            } catch (Exception e) {
                throw new RuntimeException("Hive source read error", e);
            }

        });
        context.signalNoMoreElement();
    }

    @Override
    public List<HiveSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<HiveSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {

    }
}
