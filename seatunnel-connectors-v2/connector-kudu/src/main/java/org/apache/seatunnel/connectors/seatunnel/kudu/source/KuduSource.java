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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient.KuduInputFormat;
import org.apache.seatunnel.connectors.seatunnel.kudu.state.KuduSourceState;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class KuduSource
        implements SeaTunnelSource<SeaTunnelRow, KuduSourceSplit, KuduSourceState>,
                SupportParallelism {

    private SeaTunnelRowType rowTypeInfo;
    private KuduInputFormat kuduInputFormat;
    private KuduSourceConfig kuduSourceConfig;

    public KuduSource(KuduSourceConfig kuduSourceConfig, KuduInputFormat kuduInputFormat) {
        this.kuduSourceConfig = kuduSourceConfig;
        this.kuduInputFormat = kuduInputFormat;
        this.rowTypeInfo = kuduInputFormat.getRowTypeInfo();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelRowType getProducedType() {
        return this.rowTypeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, KuduSourceSplit> createReader(
            SourceReader.Context readerContext) {
        return new KuduSourceReader(kuduInputFormat, readerContext);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> createEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext) {
        return new KuduSourceSplitEnumerator(enumeratorContext, kuduSourceConfig, kuduInputFormat);
    }

    @Override
    public SourceSplitEnumerator<KuduSourceSplit, KuduSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<KuduSourceSplit> enumeratorContext,
            KuduSourceState checkpointState) {
        return new KuduSourceSplitEnumerator(
                enumeratorContext, kuduSourceConfig, kuduInputFormat, checkpointState);
    }

    @Override
    public String getPluginName() {
        return "Kudu";
    }
}
