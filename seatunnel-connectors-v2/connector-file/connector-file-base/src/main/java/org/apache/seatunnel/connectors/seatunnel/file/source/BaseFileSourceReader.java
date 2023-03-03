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
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class BaseFileSourceReader implements SourceReader<SeaTunnelRow, FileSourceSplit> {
    private final ReadStrategy readStrategy;
    private final HadoopConf hadoopConf;
    private final SourceReader.Context context;
    private final Deque<FileSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();
    private volatile boolean noMoreSplit;

    public BaseFileSourceReader(
            ReadStrategy readStrategy, HadoopConf hadoopConf, SourceReader.Context context) {
        this.readStrategy = readStrategy;
        this.hadoopConf = hadoopConf;
        this.context = context;
    }

    @Override
    public void open() throws Exception {
        readStrategy.init(hadoopConf);
    }

    @Override
    public void close() throws IOException {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            FileSourceSplit split = sourceSplits.poll();
            if (null != split) {
                try {
                    readStrategy.read(split.splitId(), output);
                } catch (Exception e) {
                    String errorMsg =
                            String.format("Read data from this file [%s] failed", split.splitId());
                    throw new FileConnectorException(
                            CommonErrorCode.FILE_OPERATION_FAILED, errorMsg, e);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded File source");
                context.signalNoMoreElement();
            } else {
                Thread.sleep(1000L);
            }
        }
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
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
