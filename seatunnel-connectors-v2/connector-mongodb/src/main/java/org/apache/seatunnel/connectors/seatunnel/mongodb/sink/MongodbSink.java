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

import com.google.auto.service.AutoService;

import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSink.class)
public class MongodbSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private MongodbWriterOptions options;

    private SeaTunnelRowType seaTunnelRowType;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        MongodbConfig.ProcessConfig processConfig = new MongodbConfig.ProcessConfig(pluginConfig);
        MongodbWriterOptions.Builder builder =
                MongodbWriterOptions.builder()
                        .withConnectString(pluginConfig.getString(MongodbConfig.URI.key()))
                        .withDatabase(pluginConfig.getString(MongodbConfig.DATABASE.key()))
                        .withCollection(pluginConfig.getString(MongodbConfig.COLLECTION.key()));

        processConfig.processConfigValueIfPresent(
                MongodbConfig.BUFFER_FLUSH_MAX_ROWS.key(), builder::withFlushSize);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.BUFFER_FLUSH_INTERVAL.key(), builder::withBatchIntervalMs);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.PRIMARY_KEY.key(),
                value -> builder.withPrimaryKey(((List<String>) value).toArray(new String[0])));

        processConfig.processConfigValueIfPresent(
                MongodbConfig.PRIMARY_KEY.getFallbackKeys(),
                value -> builder.withPrimaryKey(((List<String>) value).toArray(new String[0])));

        processConfig.processConfigValueIfPresent(
                MongodbConfig.UPSERT_ENABLE.key(), builder::withUpsertEnable);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.RETRY_MAX.key(), builder::withRetryMax);

        processConfig.processConfigValueIfPresent(
                MongodbConfig.RETRY_INTERVAL.key(), builder::withRetryInterval);

        this.options = builder.build();
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
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) {
        return new MongodbWriter(
                new RowDataDocumentSerializer(
                        RowDataToBsonConverters.createConverter(seaTunnelRowType),
                        options,
                        new MongoKeyExtractor(options)),
                options,
                context);
    }
}
