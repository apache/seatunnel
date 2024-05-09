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

package org.apache.seatunnel.connectors.seatunnel.hive.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;
import org.apache.seatunnel.connectors.seatunnel.file.source.reader.ReadStrategy;
import org.apache.seatunnel.connectors.seatunnel.file.source.split.FileSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.MultipleTableHiveSourceConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode.FILE_READ_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorErrorCode.FILE_READ_STRATEGY_NOT_SUPPORT;

@Slf4j
public class MultipleTableHiveSourceReader implements SourceReader<SeaTunnelRow, FileSourceSplit> {

    private final SourceReader.Context context;
    private volatile boolean noMoreSplit;

    private final Deque<FileSourceSplit> sourceSplits = new ConcurrentLinkedDeque<>();

    private final Map<String, ReadStrategy> readStrategyMap;

    public MultipleTableHiveSourceReader(
            SourceReader.Context context,
            MultipleTableHiveSourceConfig multipleTableHiveSourceConfig) {
        this.context = context;
        this.readStrategyMap =
                multipleTableHiveSourceConfig.getHiveSourceConfigs().stream()
                        .collect(
                                Collectors.toMap(
                                        localFileSourceConfig ->
                                                localFileSourceConfig
                                                        .getCatalogTable()
                                                        .getTableId()
                                                        .toTablePath()
                                                        .toString(),
                                        HiveSourceConfig::getReadStrategy));
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        synchronized (output.getCheckpointLock()) {
            FileSourceSplit split = sourceSplits.poll();
            if (null != split) {
                ReadStrategy readStrategy = readStrategyMap.get(split.getTableId());
                if (readStrategy == null) {
                    throw new FileConnectorException(
                            FILE_READ_STRATEGY_NOT_SUPPORT,
                            "Cannot found the read strategy for this table: ["
                                    + split.getTableId()
                                    + "]");
                }
                try {
                    readStrategy.read(split.getFilePath(), split.getTableId(), output);
                } catch (Exception e) {
                    String errorMsg =
                            String.format("Read data from this file [%s] failed", split.splitId());
                    throw new FileConnectorException(FILE_READ_FAILED, errorMsg, e);
                }
            } else if (noMoreSplit && sourceSplits.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info(
                        "There is no more element for the bounded MultipleTableLocalFileSourceReader");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<FileSourceSplit> snapshotState(long checkpointId) {
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
    public void notifyCheckpointComplete(long checkpointId) {
        // do nothing
    }

    @Override
    public void open() throws Exception {
        // do nothing
        log.info("Opened the MultipleTableHiveSourceReader");
    }

    @Override
    public void close() throws IOException {
        // do nothing
        log.info("Closed the MultipleTableHiveSourceReader");
    }
}
