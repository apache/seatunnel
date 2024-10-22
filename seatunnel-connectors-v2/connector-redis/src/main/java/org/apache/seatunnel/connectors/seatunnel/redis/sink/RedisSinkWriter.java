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
        List<String> fields = Arrays.asList(seaTunnelRowType.getFieldNames());
        String key = getKey(element, fields);
        keyBuffer.add(key);
        String value = getValue(element, fields);
        valueBuffer.add(value);
        if (keyBuffer.size() >= batchSize) {
            doBatchWrite();
            clearBuffer();
        }
    }

    private String getKey(SeaTunnelRow element, List<String> fields) {
        String keyField = redisParameters.getKeyField();
        String[] keyFieldSegments = keyField.split(REDIS_GROUP_DELIMITER);
        StringBuilder key = new StringBuilder();
        for (int i = 0; i < keyFieldSegments.length; i++) {
            String keyFieldSegment = keyFieldSegments[i];
            if (keyFieldSegment.startsWith(LEFT_PLACEHOLDER_MARKER)
                    && keyFieldSegment.endsWith(RIGHT_PLACEHOLDER_MARKER)) {
                String realKeyField = keyFieldSegment.substring(1, keyFieldSegment.length() - 1);
                if (fields.contains(realKeyField)) {
                    key.append(element.getField(fields.indexOf(realKeyField)).toString());
                } else {
                    key.append(keyFieldSegment);
                }
            } else {
                key.append(keyFieldSegment);
            }
            if (i != keyFieldSegments.length - 1) {
                key.append(REDIS_GROUP_DELIMITER);
            }
        }
        return key.toString();
    }

    private String getValue(SeaTunnelRow element, List<String> fields) {
        String value;
        RedisDataType redisDataType = redisParameters.getRedisDataType();
        if (RedisDataType.HASH.equals(redisDataType)) {
            value = handleHashType(element, fields);
        } else {
            value = handleOtherTypes(element, fields);
        }
        if (StringUtils.isEmpty(value)) {
            byte[] serialize = serializationSchema.serialize(element);
            value = new String(serialize);
        }
        return value;
    }

    private String handleHashType(SeaTunnelRow element, List<String> fields) {
        String hashKeyColumn = redisParameters.getHashKeyColumn();
        String hashValueColumn = redisParameters.getHashValueColumn();
        if (StringUtils.isEmpty(hashKeyColumn)) {
            return "";
        }
        String hashKey;
        if (fields.contains(hashKeyColumn)) {
            hashKey = element.getField(fields.indexOf(hashKeyColumn)).toString();
        } else {
            hashKey = hashKeyColumn;
        }
        String hashValue;
        if (StringUtils.isEmpty(hashValueColumn)) {
            hashValue = new String(serializationSchema.serialize(element));
        } else {
            if (fields.contains(hashValueColumn)) {
                hashValue = element.getField(fields.indexOf(hashValueColumn)).toString();
            } else {
                hashValue = hashValueColumn;
            }
        }
        Map<String, String> kvMap = new HashMap<>();
        kvMap.put(hashKey, hashValue);
        return JsonUtils.toJsonString(kvMap);
    }

    private String handleOtherTypes(SeaTunnelRow element, List<String> fields) {
        String valueColumn = redisParameters.getValueColumn();
        if (StringUtils.isEmpty(valueColumn)) {
            return "";
        }
        if (fields.contains(valueColumn)) {
            return element.getField(fields.indexOf(valueColumn)).toString();
        }
        return valueColumn;
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
