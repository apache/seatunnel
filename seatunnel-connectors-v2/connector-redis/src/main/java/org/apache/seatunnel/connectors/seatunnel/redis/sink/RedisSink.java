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

package org.apache.seatunnel.connectors.seatunnel.redis.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public class RedisSink extends AbstractSimpleSink<SeaTunnelRow, TableSchema>
        implements SupportMultiTableSink {
    private final RedisParameters redisParameters = new RedisParameters();
    private final TableSchema tableSchema;
    private final ReadonlyConfig readonlyConfig;
    private final CatalogTable catalogTable;

    public RedisSink(ReadonlyConfig config, CatalogTable table) {
        this.readonlyConfig = config;
        this.catalogTable = table;
        this.redisParameters.buildWithConfig(config);
        this.tableSchema = catalogTable.getTableSchema();
    }

    @Override
    public String getPluginName() {
        return RedisBaseOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public RedisSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new RedisSinkWriter(tableSchema, redisParameters);
    }

    @Override
    public RedisSinkWriter restoreWriter(SinkWriter.Context context, List<TableSchema> states)
            throws IOException {
        if (states == null || states.isEmpty()) {
            return createWriter(context);
        }
        TableSchema restoredSchema = states.get(0);
        for (TableSchema state : states) {
            if (!restoredSchema.equals(state)) {
                throw new IOException("Redis sink restored inconsistent table schema states");
            }
        }
        return new RedisSinkWriter(restoredSchema, redisParameters);
    }

    @Override
    public Optional<Serializer<TableSchema>> getWriterStateSerializer() {
        return Optional.of(new DefaultSerializer<>());
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
