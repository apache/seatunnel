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
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataDocumentSerializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.RowDataToBsonConverters;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.config.MongodbWriterOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.sink.writer.MongodbBulkWriter;

import com.google.auto.service.AutoService;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSink.class)
public class MongodbSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private MongodbWriterOptions options;

    private SeaTunnelRowType seaTunnelRowType;

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (pluginConfig.hasPath(MongodbConfig.CONNECTION.key())
                && pluginConfig.hasPath(MongodbConfig.DATABASE.key())
                && pluginConfig.hasPath(MongodbConfig.COLLECTION.key())) {
            String connection = pluginConfig.getString(MongodbConfig.CONNECTION.key());
            String database = pluginConfig.getString(MongodbConfig.DATABASE.key());
            String collection = pluginConfig.getString(MongodbConfig.COLLECTION.key());
            MongodbWriterOptions.Builder builder =
                    MongodbWriterOptions.builder()
                            .withConnectString(connection)
                            .withDatabase(database)
                            .withCollection(collection)
                            .withFlushSize(
                                    pluginConfig.hasPath(MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key())
                                            ? pluginConfig.getInt(
                                                    MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key())
                                            : MongodbConfig.BUFFER_FLUSH_MAX_ROWS.defaultValue())
                            .withBatchIntervalMs(
                                    pluginConfig.hasPath(MongodbConfig.BUFFER_FLUSH_INTERVAL.key())
                                            ? pluginConfig.getLong(
                                                    MongodbConfig.BUFFER_FLUSH_INTERVAL.key())
                                            : MongodbConfig.BUFFER_FLUSH_INTERVAL.defaultValue())
                            .withUpsertKey(
                                    pluginConfig.hasPath(MongodbConfig.UPSERT_KEY.key())
                                            ? pluginConfig
                                                    .getStringList(MongodbConfig.UPSERT_KEY.key())
                                                    .toArray(new String[0])
                                            : new String[] {})
                            .withUpsertEnable(
                                    pluginConfig.hasPath(MongodbConfig.UPSERT_ENABLE.key())
                                            ? pluginConfig.getBoolean(
                                                    MongodbConfig.UPSERT_ENABLE.key())
                                            : MongodbConfig.UPSERT_ENABLE.defaultValue())
                            .withRetryMax(
                                    pluginConfig.hasPath(MongodbConfig.RETRY_MAX.key())
                                            ? pluginConfig.getInt(MongodbConfig.RETRY_MAX.key())
                                            : MongodbConfig.RETRY_MAX.defaultValue())
                            .withRetryInterval(
                                    pluginConfig.hasPath(MongodbConfig.RETRY_INTERVAL.key())
                                            ? pluginConfig.getLong(
                                                    MongodbConfig.RETRY_INTERVAL.key())
                                            : MongodbConfig.RETRY_INTERVAL.defaultValue());
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new MongodbBulkWriter(
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(seaTunnelRowType),
                        options,
                        new MongoKeyExtractor(options)),
                options);
    }
}
