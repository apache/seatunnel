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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.sender.MongoDBConnectorDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffsetFactory;

import com.google.auto.service.AutoService;
import lombok.NoArgsConstructor;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.Optional;

@NoArgsConstructor
@AutoService(SeaTunnelSource.class)
public class MongodbIncrementalSource<T> extends IncrementalSource<T, MongodbSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "MongoDB-CDC";

    public MongodbIncrementalSource(
            ReadonlyConfig options,
            SeaTunnelDataType<SeaTunnelRow> dataType,
            List<CatalogTable> catalogTables) {
        super(options, dataType, catalogTables);
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return MongodbSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return MongodbSourceOptions.STOP_MODE;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public SourceConfig.Factory<MongodbSourceConfig> createSourceConfigFactory(
            @Nonnull ReadonlyConfig config) {
        MongodbSourceConfigProvider.Builder builder =
                MongodbSourceConfigProvider.newBuilder()
                        .hosts(config.get(MongodbSourceOptions.HOSTS))
                        .validate();
        Optional.ofNullable(config.get(MongodbSourceOptions.DATABASE))
                .ifPresent(builder::databaseList);
        Optional.ofNullable(config.get(MongodbSourceOptions.COLLECTION))
                .ifPresent(builder::collectionList);
        Optional.ofNullable(config.get(MongodbSourceOptions.USERNAME)).ifPresent(builder::username);
        Optional.ofNullable(config.get(MongodbSourceOptions.PASSWORD)).ifPresent(builder::password);
        Optional.ofNullable(config.get(MongodbSourceOptions.CONNECTION_OPTIONS))
                .ifPresent(builder::connectionOptions);
        Optional.ofNullable(config.get(MongodbSourceOptions.BATCH_SIZE))
                .ifPresent(builder::batchSize);
        Optional.ofNullable(config.get(MongodbSourceOptions.POLL_MAX_BATCH_SIZE))
                .ifPresent(builder::pollMaxBatchSize);
        Optional.ofNullable(config.get(MongodbSourceOptions.POLL_AWAIT_TIME_MILLIS))
                .ifPresent(builder::pollAwaitTimeMillis);
        Optional.ofNullable(config.get(MongodbSourceOptions.HEARTBEAT_INTERVAL_MILLIS))
                .ifPresent(builder::heartbeatIntervalMillis);
        Optional.ofNullable(config.get(MongodbSourceOptions.HEARTBEAT_INTERVAL_MILLIS))
                .ifPresent(builder::splitMetaGroupSize);
        Optional.ofNullable(config.get(MongodbSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB))
                .ifPresent(builder::splitSizeMB);
        Optional.ofNullable(startupConfig).ifPresent(builder::startupOptions);
        Optional.ofNullable(stopConfig).ifPresent(builder::stopOptions);
        return builder;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        SeaTunnelDataType<SeaTunnelRow> physicalRowType;
        if (dataType == null) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(MongodbSourceOptions.DEBEZIUM_PROPERTIES));
        } else {
            physicalRowType = dataType;
            return (DebeziumDeserializationSchema<T>)
                    new MongoDBConnectorDeserializationSchema(physicalRowType, physicalRowType);
        }
    }

    @Override
    public DataSourceDialect<MongodbSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new MongodbDialect();
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new ChangeStreamOffsetFactory();
    }
}
