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

import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;
import org.apache.seatunnel.translation.spark.source.partition.continuous.source.endpoint.EndpointSplitEnumeratorContext;
import org.apache.seatunnel.translation.spark.utils.CaseInsensitiveStringMap;

import org.apache.spark.rpc.RpcEndpointRef;
import org.apache.spark.sql.connector.read.InputPartition;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReaderFactory;
import org.apache.spark.sql.connector.read.streaming.ContinuousStream;
import org.apache.spark.sql.connector.read.streaming.Offset;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;
import org.apache.spark.sql.execution.streaming.continuous.seatunnel.rpc.SplitEnumeratorEndpoint;

public class SeaTunnelContinuousStream implements ContinuousStream {
    private final SeaTunnelSource<SeaTunnelRow, SourceSplit, ?> source;
    private final int parallelism;
    private final String jobId;
    private final String checkpointLocation;
    private final CaseInsensitiveStringMap caseInsensitiveStringMap;
    private final MultiTableManager multiTableManager;
    private RpcEndpointRef endpointRef;

    public SeaTunnelContinuousStream(
            SeaTunnelSource<SeaTunnelRow, SourceSplit, ?> source,
            int parallelism,
            String jobId,
            String checkpointLocation,
            CaseInsensitiveStringMap caseInsensitiveStringMap,
            MultiTableManager multiTableManager) {
        this.source = source;
        this.parallelism = parallelism;
        this.jobId = jobId;
        this.checkpointLocation = checkpointLocation;
        this.caseInsensitiveStringMap = caseInsensitiveStringMap;
        this.multiTableManager = multiTableManager;
    }

    @Override
    public InputPartition[] planInputPartitions(Offset start) {
        SourceSplitEnumerator.Context<SourceSplit> enumeratorContext =
                new EndpointSplitEnumeratorContext<>(parallelism, jobId);

        try {
            SourceSplitEnumerator enumerator = source.createEnumerator(enumeratorContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        SplitEnumeratorEndpoint enumeratorEndpoint = new SplitEnumeratorEndpoint();
        String endpointName = "SplitEnumeratorEndpoint-" + java.util.UUID.randomUUID();
        endpointRef = enumeratorEndpoint.rpcEnv().setupEndpoint(endpointName, enumeratorEndpoint);
        InputPartition[] inputPartitions = new SeaTunnelInputPartition[parallelism];
        for (int idx = 0; idx < inputPartitions.length; idx++) {
            inputPartitions[idx] = new SeaTunnelInputPartition(endpointName, idx);
        }
        return inputPartitions;
    }

    @Override
    public ContinuousPartitionReaderFactory createContinuousReaderFactory() {
        return null;
    }

    @Override
    public Offset mergeOffsets(PartitionOffset[] offsets) {
        return null;
    }

    @Override
    public boolean needsReconfiguration() {
        return ContinuousStream.super.needsReconfiguration();
    }

    @Override
    public Offset initialOffset() {
        return null;
    }

    @Override
    public Offset deserializeOffset(String json) {
        return null;
    }

    @Override
    public void commit(Offset end) {}

    @Override
    public void stop() {}
}
