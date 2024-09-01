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

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.read.streaming.ContinuousPartitionReader;
import org.apache.spark.sql.connector.read.streaming.PartitionOffset;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class SeaTunnelContinuousPartitionReader<
                SplitT extends SourceSplit, StateT extends Serializable>
        implements ContinuousPartitionReader<InternalRow> {
    private final SeaTunnelSource<SeaTunnelRow, ?, ?> source;
    private final String jobId;
    private final Integer subtaskId;
    private final Integer parallelism;
    private final SourceSplitEnumerator splitEnumerator;
    private final SourceSplitEnumerator.Context splitEnumeratorCtx;
    protected final List<SplitT> restoredSplitState;
    protected final SourceReader<SeaTunnelRow, SplitT> reader;

    protected final Serializer<SplitT> splitSerializer;
    protected final Serializer<StateT> enumeratorStateSerializer;
    private InternalMultiRowCollector collector;
    Handover<InternalRow> handover;

    public SeaTunnelContinuousPartitionReader(
            SeaTunnelSource<SeaTunnelRow, ?, ?> source,
            String jobId,
            Integer subtaskId,
            Integer parallelism,
            SourceSplitEnumerator splitEnumerator,
            SourceSplitEnumerator.Context splitEnumeratorCtx,
            List<SplitT> restoredSplitState,
            SourceReader<SeaTunnelRow, SplitT> reader,
            Serializer<SplitT> splitSerializer,
            Serializer<StateT> enumeratorStateSerializer,
            int subTaskId) {
        this.source = source;
        this.jobId = jobId;
        this.subtaskId = subtaskId;
        this.parallelism = parallelism;
        this.splitEnumerator = splitEnumerator;
        this.splitEnumeratorCtx = splitEnumeratorCtx;
        this.restoredSplitState = restoredSplitState;
        this.reader = reader;
        this.splitSerializer = splitSerializer;
        this.enumeratorStateSerializer = enumeratorStateSerializer;
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
                    // splitEnumeratorCtx.assignSplit();
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
