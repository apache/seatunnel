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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class MaxcomputeSourceReader implements SourceReader<SeaTunnelRow, MaxcomputeSourceSplit> {
    private final SourceReader.Context context;
    private final Queue<MaxcomputeSourceSplit> sourceSplits;
    private final ReadonlyConfig readonlyConfig;
    private volatile boolean noMoreSplit;
    private final Map<TablePath, SourceTableInfo> sourceTableInfos;

    public MaxcomputeSourceReader(
            ReadonlyConfig readonlyConfig,
            SourceReader.Context context,
            Map<TablePath, SourceTableInfo> sourceTableInfos) {
        this.readonlyConfig = readonlyConfig;
        this.context = context;
        this.sourceSplits = new ConcurrentLinkedDeque<>();
        this.sourceTableInfos = sourceTableInfos;
    }

    @Override
    public void open() {}

    @Override
    public void close() {}

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        MaxcomputeSourceSplit split = sourceSplits.poll();
        if (split != null) {
            synchronized (output.getCheckpointLock()) {
                try {
                    TableTunnel.DownloadSession session =
                            MaxcomputeUtil.getDownloadSession(
                                    readonlyConfig,
                                    sourceTableInfos
                                            .get(split.getTablePath())
                                            .getCatalogTable()
                                            .getTablePath(),
                                    sourceTableInfos.get(split.getTablePath()).getPartitionSpec());
                    TunnelRecordReader recordReader =
                            session.openRecordReader(split.getRowStart(), split.getRowNum());
                    log.info("open record reader success");
                    Record record;
                    while ((record = recordReader.read()) != null) {
                        SeaTunnelRow seaTunnelRow =
                                MaxcomputeTypeMapper.getSeaTunnelRowData(
                                        record,
                                        sourceTableInfos
                                                .get(split.getTablePath())
                                                .getCatalogTable()
                                                .getSeaTunnelRowType());
                        seaTunnelRow.setTableId(
                                sourceTableInfos
                                        .get(split.getTablePath())
                                        .getCatalogTable()
                                        .getTablePath()
                                        .toString());
                        output.collect(seaTunnelRow);
                    }
                    recordReader.close();
                } catch (Exception e) {
                    throw new MaxcomputeConnectorException(
                            CommonErrorCodeDeprecated.READER_OPERATION_FAILED, e);
                }
            }
        }
        if (this.sourceSplits.isEmpty()
                && this.noMoreSplit
                && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded Maxcompute source");
            context.signalNoMoreElement();
        } else if (this.sourceSplits.isEmpty() && !this.noMoreSplit) {
            context.sendSplitRequest();
        }
    }

    @Override
    public List<MaxcomputeSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(sourceSplits);
    }

    @Override
    public void addSplits(List<MaxcomputeSourceSplit> splits) {
        sourceSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        this.noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}
}
