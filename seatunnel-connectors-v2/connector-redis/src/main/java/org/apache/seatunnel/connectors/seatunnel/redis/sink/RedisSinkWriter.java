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

import org.apache.seatunnel.api.serialization.SerializationSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class RedisSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final RedisParameters redisParameters;
    private final SerializationSchema serializationSchema;
    private final Jedis jedis;

    public RedisSinkWriter(SeaTunnelRowType seaTunnelRowType, RedisParameters redisParameters) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.redisParameters = redisParameters;
        // TODO according to format to initialize serializationSchema
        // Now temporary using json serializationSchema
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.jedis = redisParameters.buildJedis();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String data = new String(serializationSchema.serialize(element));
        RedisDataType redisDataType = redisParameters.getRedisDataType();
        String keyField = redisParameters.getKeyField();
        List<String> fields = Arrays.asList(seaTunnelRowType.getFieldNames());
        String key;
        if (fields.contains(keyField)) {
            key = element.getField(fields.indexOf(keyField)).toString();
        } else {
            key = keyField;
        }
        long expire = redisParameters.getExpire();
        redisDataType.set(jedis, key, data, expire);
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(jedis)) {
            jedis.close();
        }
    }
}
