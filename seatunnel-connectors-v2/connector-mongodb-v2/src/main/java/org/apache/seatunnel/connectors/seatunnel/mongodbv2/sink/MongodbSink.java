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

package org.apache.seatunnel.connectors.seatunnel.mongodbv2.sink;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde.DocumentSerializer;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.mongodbv2.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSink.class)
public class MongodbSink
        implements SeaTunnelSink<SeaTunnelRow, DocumentBulk, MongodbCommitInfo, Void> {
    @Override
    public SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> restoreWriter(SinkWriter.Context context, List<DocumentBulk> states) throws IOException {
        return new MongoBulkWriter(clientProvider, new RowDataDocumentSerializer(seaTunnelRowType), options);
    }

    @Override
    public Optional<Serializer<DocumentBulk>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<SinkCommitter<MongodbCommitInfo>> createCommitter() throws IOException {
        return Optional.of(new MongoCommitter(clientProvider,false, new String[]{}));
    }

    @Override
    public Optional<Serializer<MongodbCommitInfo>> getCommitInfoSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    private MongoClientProvider clientProvider;
    private DocumentSerializer<SeaTunnelRow> serializer;

    private MongoConnectorOptions options;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        MongoConnectorOptions.Builder builder = MongoConnectorOptions.builder()
                .withConnectString("mongodb://localhost:27017/test?retryWrites=true&writeConcern=majority")
                .withDatabase("sink")
                .withCollection("test1")
                .withTransactionEnable(true)
                .withFlushOnCheckpoint(true)
                .withFlushSize(1000)
                .withFlushInterval(Duration.of(30_000L, ChronoUnit.MILLIS))
                .withUpsertEnable(false)
                .withUpsertKey(new String[]{});
        this.options = builder.build();
        this.clientProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(
                                "mongodb://localhost:27017/test?retryWrites=true&writeConcern=majority")
                        .database("sink")
                        .collection("test1")
                        .build();
    }

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }


    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public SinkWriter<SeaTunnelRow, MongodbCommitInfo, DocumentBulk> createWriter(
            SinkWriter.Context context) throws IOException {
        return new MongoBulkWriter(clientProvider, new RowDataDocumentSerializer(seaTunnelRowType), options);
    }
}
