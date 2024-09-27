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

package org.apache.seatunnel.translation.spark.sink;

import org.apache.seatunnel.api.sink.MultiTableResourceManager;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SupportResourceShare;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.sink.write.SeaTunnelSparkDataWriterFactory;
import org.apache.seatunnel.translation.spark.sink.write.SeaTunnelSparkWriterCommitMessage;

import org.apache.spark.sql.connector.write.BatchWrite;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.PhysicalWriteInfo;
import org.apache.spark.sql.connector.write.WriterCommitMessage;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingWrite;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SeaTunnelBatchWrite<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements BatchWrite, StreamingWrite {

    private final SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink;

    private final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> aggregatedCommitter;

    private MultiTableResourceManager resourceManager;

    private final CatalogTable[] catalogTables;

    private final String jobId;

    private final int parallelism;

    public SeaTunnelBatchWrite(
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable[] catalogTables,
            String jobId,
            int parallelism)
            throws IOException {
        this.sink = sink;
        this.catalogTables = catalogTables;
        this.jobId = jobId;
        this.parallelism = parallelism;
        this.aggregatedCommitter = sink.createAggregatedCommitter().orElse(null);
        if (aggregatedCommitter != null) {
            if (this.aggregatedCommitter instanceof SupportResourceShare) {
                resourceManager =
                        ((SupportResourceShare) this.aggregatedCommitter)
                                .initMultiTableResourceManager(1, 1);
            }
            aggregatedCommitter.init();
            if (resourceManager != null) {
                ((SupportResourceShare) this.aggregatedCommitter)
                        .setMultiTableResourceManager(resourceManager, 0);
            }
        }
    }

    @Override
    public DataWriterFactory createBatchWriterFactory(PhysicalWriteInfo info) {
        return new SeaTunnelSparkDataWriterFactory<>(sink, catalogTables, jobId, parallelism);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (aggregatedCommitter != null) {
            try {
                aggregatedCommitter.commit(combineCommitMessage(messages));
            } catch (IOException e) {
                throw new RuntimeException("SinkAggregatedCommitter commit failed in driver", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        if (aggregatedCommitter != null) {
            try {
                aggregatedCommitter.abort(combineCommitMessage(messages));
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }

    @Override
    public StreamingDataWriterFactory createStreamingWriterFactory(PhysicalWriteInfo info) {
        return (StreamingDataWriterFactory) createBatchWriterFactory(info);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        commit(messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        abort(messages);
    }

    private List<AggregatedCommitInfoT> combineCommitMessage(WriterCommitMessage[] messages) {
        if (aggregatedCommitter == null || messages.length == 0) {
            return Collections.emptyList();
        }
        List<CommitInfoT> commitInfos =
                Arrays.stream(messages)
                        .map(m -> ((SeaTunnelSparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
        return Collections.singletonList(aggregatedCommitter.combine(commitInfos));
    }
}
