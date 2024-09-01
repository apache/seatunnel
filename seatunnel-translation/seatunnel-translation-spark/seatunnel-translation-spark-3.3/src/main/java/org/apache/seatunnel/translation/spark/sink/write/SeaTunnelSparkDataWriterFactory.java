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

package org.apache.seatunnel.translation.spark.sink.write;

import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.translation.spark.execution.MultiTableManager;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.write.DataWriter;
import org.apache.spark.sql.connector.write.DataWriterFactory;
import org.apache.spark.sql.connector.write.streaming.StreamingDataWriterFactory;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.util.Map;

public class SeaTunnelSparkDataWriterFactory<CommitInfoT, StateT>
        implements DataWriterFactory, StreamingDataWriterFactory {

    private final SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, ?> sink;
    private final Map<String, String> properties;
    private final CatalogTable[] catalogTables;
    private final StructType schema;
    private final String checkpointLocation;
    private final String jobId;

    public SeaTunnelSparkDataWriterFactory(
            SeaTunnelSink<SeaTunnelRow, StateT, CommitInfoT, ?> sink,
            Map<String, String> properties,
            CatalogTable[] catalogTables,
            StructType schema,
            String checkpointLocation,
            String jobId) {
        this.sink = sink;
        this.properties = properties;
        this.catalogTables = catalogTables;
        this.schema = schema;
        this.checkpointLocation = checkpointLocation;
        this.jobId = jobId;
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId) {
        SinkWriter.Context context = new DefaultSinkWriterContext(jobId, (int) taskId);
        SinkWriter<SeaTunnelRow, CommitInfoT, StateT> writer;
        SinkCommitter<CommitInfoT> committer;
        try {
            writer = sink.createWriter(context);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkWriter.", e);
        }
        try {
            committer = sink.createCommitter().orElse(null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkCommitter.", e);
        }
        return new SeaTunnelSparkDataWriter<>(
                writer,
                committer,
                new MultiTableManager(catalogTables),
                schema,
                properties,
                checkpointLocation,
                partitionId,
                0,
                context);
    }

    @Override
    public DataWriter<InternalRow> createWriter(int partitionId, long taskId, long epochId) {
        SinkWriter.Context context = new DefaultSinkWriterContext(jobId, (int) taskId);
        SinkWriter<SeaTunnelRow, CommitInfoT, StateT> writer;
        SinkCommitter<CommitInfoT> committer;
        try {
            writer = sink.createWriter(context);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkWriter.", e);
        }
        try {
            committer = sink.createCommitter().orElse(null);
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SinkCommitter.", e);
        }
        return new SeaTunnelSparkDataWriter<>(
                writer,
                committer,
                new MultiTableManager(catalogTables),
                schema,
                properties,
                checkpointLocation,
                partitionId,
                epochId,
                context);
    }
}
