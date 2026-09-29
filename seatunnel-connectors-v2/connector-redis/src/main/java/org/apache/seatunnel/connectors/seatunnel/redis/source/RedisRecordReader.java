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

package org.apache.seatunnel.connectors.seatunnel.redis.source;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisTableConfig;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class RedisRecordReader {
    protected final RedisParameters redisParameters;
    protected final RedisTableConfig tableConfig;
    protected final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    protected final TablePath tablePath;
    protected RedisClient redisClient;

    protected RedisRecordReader(
            RedisParameters redisParameters,
            RedisTableConfig tableConfig,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            RedisClient redisClient,
            TablePath tablePath) {
        this.redisParameters = redisParameters;
        this.tableConfig = tableConfig;
        this.deserializationSchema = deserializationSchema;
        this.redisClient = redisClient;
        this.tablePath = tablePath;
    }

    /**
     * Helper method to create SeaTunnelRow with tableId set. This is required for multi-table mode
     * to work correctly.
     */
    protected SeaTunnelRow createRowWithTableId(Object[] fields) {
        SeaTunnelRow row = new SeaTunnelRow(fields);
        row.setTableId(tablePath.toString());
        return row;
    }

    /** Helper method to set tableId for a SeaTunnelRow. */
    protected void setTableId(SeaTunnelRow row) {
        row.setTableId(tablePath.toString());
    }

    public void pollHashMapToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<Map<String, String>> values = redisClient.batchGetHash(keys, tableConfig);
        if (deserializationSchema == null) {
            for (Map<String, String> value : values) {
                output.collect(createRowWithTableId(new Object[] {JsonUtils.toJsonString(value)}));
            }
            return;
        }
        RedisSourceOptions.HashKeyParseMode hashKeyParseMode = tableConfig.getHashKeyParseMode();
        for (Map<String, String> recordsMap : values) {
            if (hashKeyParseMode == RedisSourceOptions.HashKeyParseMode.KV) {
                SeaTunnelRow row =
                        deserializationSchema.deserialize(
                                JsonUtils.toJsonString(recordsMap).getBytes());
                setTableId(row);
                output.collect(row);
            } else {
                output.collect(
                        createRowWithTableId(new Object[] {JsonUtils.toJsonString(recordsMap)}));
            }
        }
    }

    public abstract void pollZsetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException;

    public abstract void pollSetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException;

    public abstract void pollListToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException;

    public abstract void pollStringToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException;
}
