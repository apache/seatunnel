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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataToBsonConverters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.commit.MongodbSinkAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.DocumentBulk;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.state.MongodbCommitInfo;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSink.class)
public class MongodbSink
        implements SeaTunnelSink<
                SeaTunnelRow, DocumentBulk, MongodbCommitInfo, MongodbAggregatedCommitInfo> {

    private MongodbWriterOptions options;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (pluginConfig.hasPath(MongodbConfig.URI.key())
                && pluginConfig.hasPath(MongodbConfig.DATABASE.key())
                && pluginConfig.hasPath(MongodbConfig.COLLECTION.key())) {
            String connection = pluginConfig.getString(MongodbConfig.URI.key());
            String database = pluginConfig.getString(MongodbConfig.DATABASE.key());
            String collection = pluginConfig.getString(MongodbConfig.COLLECTION.key());
            MongodbWriterOptions.Builder builder =
                    MongodbWriterOptions.builder()
                            .withConnectString(connection)
                            .withDatabase(database)
                            .withCollection(collection);
            if (pluginConfig.hasPath(MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key())) {
                builder.withFlushSize(
                        pluginConfig.getInt(MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.BUFFER_FLUSH_INTERVAL.key())) {
                builder.withBatchIntervalMs(
                        pluginConfig.getLong(MongodbConfig.BUFFER_FLUSH_INTERVAL.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.PRIMARY_KEY.key())) {
                builder.withPrimaryKey(
                        pluginConfig
                                .getStringList(MongodbConfig.PRIMARY_KEY.key())
                                .toArray(new String[0]));
            }
            List<String> fallbackKeys = MongodbConfig.PRIMARY_KEY.getFallbackKeys();
            fallbackKeys.forEach(
                    key -> {
                        if (pluginConfig.hasPath(key)) {
                            builder.withPrimaryKey(
                                    pluginConfig.getStringList(key).toArray(new String[0]));
                        }
                    });
            if (pluginConfig.hasPath(MongodbConfig.UPSERT_ENABLE.key())) {
                builder.withUpsertEnable(
                        pluginConfig.getBoolean(MongodbConfig.UPSERT_ENABLE.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.RETRY_MAX.key())) {
                builder.withRetryMax(pluginConfig.getInt(MongodbConfig.RETRY_MAX.key()));
            }
            if (pluginConfig.hasPath(MongodbConfig.RETRY_INTERVAL.key())) {
                builder.withRetryInterval(pluginConfig.getLong(MongodbConfig.RETRY_INTERVAL.key()));
            }

            if (pluginConfig.hasPath(MongodbConfig.TRANSACTION.key())) {
                builder.withTransaction(pluginConfig.getBoolean(MongodbConfig.TRANSACTION.key()));
            }
            this.options = builder.build();
        }
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
            SinkWriter.Context context) {
        return new MongodbWriter(
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(seaTunnelRowType),
                        options,
                        new MongoKeyExtractor(options)),
                options,
                context);
    }

    @Override
    public Optional<Serializer<DocumentBulk>> getWriterStateSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<SinkAggregatedCommitter<MongodbCommitInfo, MongodbAggregatedCommitInfo>>
            createAggregatedCommitter() {
        return options.transaction
                ? Optional.of(new MongodbSinkAggregatedCommitter(options))
                : Optional.empty();
    }

    @Override
    public Optional<Serializer<MongodbAggregatedCommitInfo>> getAggregatedCommitInfoSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }

    @Override
    public Optional<Serializer<MongodbCommitInfo>> getCommitInfoSerializer() {
        return options.transaction ? Optional.of(new DefaultSerializer<>()) : Optional.empty();
    }
}
