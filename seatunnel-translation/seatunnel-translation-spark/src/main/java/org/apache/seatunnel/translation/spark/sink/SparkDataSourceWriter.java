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

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class SparkDataSourceWriter<CommitInfoT, StateT, AggregatedCommitInfoT> implements DataSourceWriter {

    private final SinkWriter.Context context;
    @Nullable
    private final SinkCommitter<CommitInfoT> sinkCommitter;
    @Nullable
    private final SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> sinkAggregatedCommitter;
    private final StructType schema;
    private final String sinkString;

    SparkDataSourceWriter(SinkWriter.Context context, @Nullable SinkCommitter<CommitInfoT> sinkCommitter,
                          @Nullable SinkAggregatedCommitter<CommitInfoT, AggregatedCommitInfoT> sinkAggregatedCommitter,
                          StructType schema, String sinkString) {
        this.context = context;
        this.sinkCommitter = sinkCommitter;
        this.sinkAggregatedCommitter = sinkAggregatedCommitter;
        this.sinkString = sinkString;
        this.schema = schema;
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return new SparkDataWriterFactory(context, schema, sinkString);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        if (sinkAggregatedCommitter != null) {
            try {
                sinkAggregatedCommitter.commit(combineCommitMessage(extractCommitInfo(messages)));
            } catch (IOException e) {
                throw new RuntimeException("commit failed in driver", e);
            }
        }
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        final List<CommitInfoT> commitInfos = extractCommitInfo(messages);
        if (sinkCommitter != null) {
            try {
                sinkCommitter.abort(commitInfos);
            } catch (IOException e) {
                throw new RuntimeException("SinkCommitter abort failed in driver", e);
            }
        }
        if (sinkAggregatedCommitter != null) {
            try {
                sinkAggregatedCommitter.abort(combineCommitMessage(commitInfos));
            } catch (Exception e) {
                throw new RuntimeException("SinkAggregatedCommitter abort failed in driver", e);
            }
        }
    }

    private @Nonnull List<CommitInfoT> extractCommitInfo(WriterCommitMessage[] messages) {
        return Arrays.stream(messages).filter(Objects::nonNull)
                .map(m -> ((SparkWriterCommitMessage<CommitInfoT>) m).getMessage())
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private @Nonnull List<AggregatedCommitInfoT> combineCommitMessage(List<CommitInfoT> commitInfos) {
        return Collections.singletonList(sinkAggregatedCommitter.combine(commitInfos));
    }
}
