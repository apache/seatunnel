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

package org.apache.seatunnel.translation.spark.source.partition.continuous;

import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.translation.spark.serialization.InternalMultiRowCollector;
import org.apache.seatunnel.translation.spark.source.partition.continuous.source.rpc.RpcSourceReaderContext;
import org.apache.seatunnel.translation.spark.source.partition.continuous.source.rpc.RpcSplitEnumeratorContext;

import org.apache.spark.SparkEnv;
import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.util.RpcUtils;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class SeaTunnelContinuousPartitionReader<
                SplitT extends SourceSplit, StateT extends Serializable>
        implements ContinuousPartitionReader<InternalRow> {
    private final SeaTunnelSource<SeaTunnelRow, SourceSplit, ?> source;
    private final SeaTunnelInputPartition inputPartition;
    private final RpcEndpointRef driverRef;
    protected List<SplitT> restoredSplitState;
    protected SourceReader<SeaTunnelRow, SourceSplit> reader;
    protected SourceSplitEnumerator<SourceSplit, Serializable> splitEnumerator;

    protected Serializer<SplitT> splitSerializer;
    protected Serializer<StateT> enumeratorStateSerializer;
    private InternalMultiRowCollector collector;
    Handover<InternalRow> handover;

    public SeaTunnelContinuousPartitionReader(
            SeaTunnelSource<SeaTunnelRow, SourceSplit, ?> source,
            SeaTunnelInputPartition inputPartition) {
        this.source = source;
        this.inputPartition = inputPartition;
        this.driverRef =
                RpcUtils.makeDriverRef(
                        inputPartition.getEndpointName(),
                        SparkEnv.get().conf(),
                        SparkEnv.get().rpcEnv());
        RpcSourceReaderContext readerCtx = new RpcSourceReaderContext(this.driverRef);
        RpcSplitEnumeratorContext<SourceSplit> splitEnumeratorContext =
                new RpcSplitEnumeratorContext<SourceSplit>(this.driverRef);
        try {
            reader = (SourceReader<SeaTunnelRow, SourceSplit>) this.source.createReader(readerCtx);
            splitEnumerator =
                    (SourceSplitEnumerator<SourceSplit, Serializable>)
                            this.source.createEnumerator(splitEnumeratorContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public PartitionOffset getOffset() {
        return null;
    }

    @Override
    public boolean next() throws IOException {
        try {
            if (handover.isEmpty()) {
                reader.pollNext(collector);
                if (handover.isEmpty()) {
                    splitEnumerator.run();
                }
            }
            return true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InternalRow get() {
        return null;
    }

    @Override
    public void close() throws IOException {}
}
