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
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RedisSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {
    private final SeaTunnelRowType seaTunnelRowType;
    private final RedisParameters redisParameters;
    private final SerializationSchema serializationSchema;
    private final RedisClient redisClient;

    private final int batchSize;

    private final List<String> keyBuffer;
    private final List<String> valueBuffer;

    public RedisSinkWriter(SeaTunnelRowType seaTunnelRowType, RedisParameters redisParameters) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.redisParameters = redisParameters;
        // TODO according to format to initialize serializationSchema
        // Now temporary using json serializationSchema
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.redisClient = redisParameters.buildRedisClient();
        this.batchSize = redisParameters.getBatchSize();
        this.keyBuffer = new ArrayList<>(batchSize);
        this.valueBuffer = new ArrayList<>(batchSize);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String data = new String(serializationSchema.serialize(element));
        String keyField = redisParameters.getKeyField();
        List<String> fields = Arrays.asList(seaTunnelRowType.getFieldNames());
        String key;
        if (fields.contains(keyField)) {
            key = element.getField(fields.indexOf(keyField)).toString();
        } else {
            key = keyField;
        }
        keyBuffer.add(key);
        valueBuffer.add(data);
        if (keyBuffer.size() >= batchSize) {
            doBatchWrite();
            clearBuffer();
        }
    }

    private void clearBuffer() {
        keyBuffer.clear();
        valueBuffer.clear();
    }

    private void doBatchWrite() {
        RedisDataType redisDataType = redisParameters.getRedisDataType();
        if (RedisDataType.KEY.equals(redisDataType) || RedisDataType.STRING.equals(redisDataType)) {
            redisClient.batchWriteString(keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.LIST.equals(redisDataType)) {
            redisClient.batchWriteList(keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.SET.equals(redisDataType)) {
            redisClient.batchWriteSet(keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.HASH.equals(redisDataType)) {
            redisClient.batchWriteHash(keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        if (RedisDataType.ZSET.equals(redisDataType)) {
            redisClient.batchWriteZset(keyBuffer, valueBuffer, redisParameters.getExpire());
            return;
        }
        throw new RedisConnectorException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                "UnSupport redisDataType,only support string,list,hash,set,zset");
    }

    @Override
    public void close() throws IOException {
        if (!keyBuffer.isEmpty()) {
            doBatchWrite();
            clearBuffer();
        }
    }
}
