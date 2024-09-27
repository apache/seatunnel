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

package org.apache.seatunnel.translation.spark.sink.writer;

import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter;

import java.io.IOException;

public class SparkStreamWriter<StateT, CommitInfoT, AggregatedCommitInfoT>
        extends SparkDataSourceWriter<StateT, CommitInfoT, AggregatedCommitInfoT>
        implements StreamWriter {

    public SparkStreamWriter(
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, AggregatedCommitInfoT> sink,
            CatalogTable[] catalogTables,
            String jobId,
            int parallelism)
            throws IOException {
        super(sink, catalogTables, jobId, parallelism);
    }

    @Override
    public void commit(long epochId, WriterCommitMessage[] messages) {
        super.commit(messages);
    }

    @Override
    public void abort(long epochId, WriterCommitMessage[] messages) {
        super.abort(messages);
    }

    @Override
    public void commit(WriterCommitMessage[] messages) {
        StreamWriter.super.commit(messages);
    }

    @Override
    public void abort(WriterCommitMessage[] messages) {
        StreamWriter.super.abort(messages);
    }

    @Override
    public DataWriterFactory<InternalRow> createWriterFactory() {
        return super.createWriterFactory();
    }

    @Override
    public boolean useCommitCoordinator() {
        return StreamWriter.super.useCommitCoordinator();
    }

    @Override
    public void onDataWriterCommit(WriterCommitMessage message) {
        StreamWriter.super.onDataWriterCommit(message);
    }
}
