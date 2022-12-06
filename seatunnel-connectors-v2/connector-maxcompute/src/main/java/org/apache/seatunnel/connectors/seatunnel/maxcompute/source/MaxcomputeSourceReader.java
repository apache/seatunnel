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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.exception.MaxcomputeConnectorException;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeTypeMapper;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.util.MaxcomputeUtil;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.TunnelRecordReader;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Slf4j
public class MaxcomputeSourceReader implements SourceReader<SeaTunnelRow, MaxcomputeSourceSplit> {
    private final SourceReader.Context context;
    private final Set<MaxcomputeSourceSplit> sourceSplits;
    private Config pluginConfig;
    boolean noMoreSplit;
    private SeaTunnelRowType seaTunnelRowType;

    public MaxcomputeSourceReader(Config pluginConfig, SourceReader.Context context, SeaTunnelRowType seaTunnelRowType) {
        this.pluginConfig = pluginConfig;
        this.context = context;
        this.sourceSplits = new HashSet<>();
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        sourceSplits.forEach(source -> {
            try {
                TableTunnel.DownloadSession session = MaxcomputeUtil.getDownloadSession(pluginConfig);
                TunnelRecordReader recordReader = session.openRecordReader(source.getSplitId(), source.getRowNum());
                log.info("open record reader success");
                Record record;
                while ((record = recordReader.read()) != null) {
                    SeaTunnelRow seaTunnelRow = MaxcomputeTypeMapper.getSeaTunnelRowData(record, seaTunnelRowType);
                    output.collect(seaTunnelRow);
                }
                recordReader.close();
            } catch (Exception e) {
                throw new MaxcomputeConnectorException(CommonErrorCode.READER_OPERATION_FAILED, e);
            }
        });
        if (this.noMoreSplit && Boundedness.BOUNDED.equals(context.getBoundedness())) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded Maxcompute source");
            context.signalNoMoreElement();
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
    public void notifyCheckpointComplete(long checkpointId) {
    }
}
