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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisConfig;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RedisSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final RedisParameters redisParameters;
    private final SingleSplitReaderContext context;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private Jedis jedis;

    public RedisSourceReader(RedisParameters redisParameters, SingleSplitReaderContext context, DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.redisParameters = redisParameters;
        this.context = context;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open() throws Exception {
        this.jedis = redisParameters.buildJedis();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(jedis)) {
            jedis.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        Set<String> keys = jedis.keys(redisParameters.getKeysPattern());
        RedisDataType redisDataType = redisParameters.getRedisDataType();
        for (String key : keys) {
            List<String> values = redisDataType.get(jedis, key);
            for (String value : values) {
                if (deserializationSchema == null) {
                    output.collect(new SeaTunnelRow(new Object[]{value}));
                } else {
                    if (redisParameters.getHashKeyParseMode() == RedisConfig.HashKeyParseMode.KV &&
                            redisDataType == RedisDataType.HASH) {
                        // Treat each key-value pair in the hash-key as one piece of data
                        Map<String, String> recordsMap = JsonUtils.toMap(value);
                        for (Map.Entry<String, String> entry : recordsMap.entrySet()) {
                            String k = entry.getKey();
                            String v = entry.getValue();
                            Map<String, String> valuesMap = JsonUtils.toMap(v);
                            SeaTunnelDataType<SeaTunnelRow> seaTunnelRowType = deserializationSchema.getProducedType();
                            valuesMap.put(((SeaTunnelRowType) seaTunnelRowType).getFieldName(0), k);
                            deserializationSchema.deserialize(JsonUtils.toJsonString(valuesMap).getBytes(), output);
                        }
                    } else {
                        deserializationSchema.deserialize(value.getBytes(), output);
                    }
                }
            }
        }
        context.signalNoMoreElement();
    }
}
