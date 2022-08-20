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

package org.apache.seatunnel.connectors.seatunnel.file.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class BaseFileSourceReader implements SourceReader<SeaTunnelRow, FileSourceSplit> {
    private static final long THREAD_WAIT_TIME = 500L;
    private final ReadStrategy readStrategy;
    private final HadoopConf hadoopConf;
    private final Context context;
    private final Set<FileSourceSplit> sourceSplits;

    public BaseFileSourceReader(ReadStrategy readStrategy, HadoopConf hadoopConf, Context context) {
        this.readStrategy = readStrategy;
        this.hadoopConf = hadoopConf;
        this.context = context;
        this.sourceSplits = new HashSet<>();
    }

    @Override
    public void open() throws Exception {
        readStrategy.init(hadoopConf);
    }

    @Override
    public void close() throws IOException {

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
                throw new RuntimeException("File source read error", e);
            }
        });
        context.signalNoMoreElement();
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<FileSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {

    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {

    }
}
