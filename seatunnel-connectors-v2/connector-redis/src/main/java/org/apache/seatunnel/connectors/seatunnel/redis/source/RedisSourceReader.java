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
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class RedisSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final RedisParameters redisParameters;
    private final SingleSplitReaderContext context;
    private final DeserializationSchema<SeaTunnelRow> deserializationSchema;
    private RedisClient redisClient;

    public RedisSourceReader(
            RedisParameters redisParameters,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema) {
        this.redisParameters = redisParameters;
        this.context = context;
        this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open() throws Exception {
        this.redisClient = redisParameters.buildRedisClient();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(redisClient)) {
            redisClient.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        RedisDataType redisDataType = resolveScanType(redisParameters.getRedisDataType());
        String cursor = ScanParams.SCAN_POINTER_START;
        String keysPattern = redisParameters.getKeysPattern();
        int batchSize = redisParameters.getBatchSize();
        while (true) {
            // String cursor, int batchSize, String keysPattern, RedisType type
            ScanResult<String> scanResult =
                    redisClient.scanKeys(cursor, batchSize, keysPattern, redisDataType);
            cursor = scanResult.getCursor();
            List<String> keys = scanResult.getResult();
            pollNext(keys, redisDataType, output);
            // when cursor return "0", scan end
            if (ScanParams.SCAN_POINTER_START.equals(cursor)) {
                break;
            }
        }
        context.signalNoMoreElement();
    }

    private void pollNext(List<String> keys, RedisDataType dataType, Collector<SeaTunnelRow> output)
            throws IOException {
        if (CollectionUtils.isEmpty(keys)) {
            return;
        }
        if (RedisDataType.HASH.equals(dataType)) {
            pollHashMapToNext(keys, output);
            return;
        }
        if (RedisDataType.STRING.equals(dataType) || RedisDataType.KEY.equals(dataType)) {
            pollStringToNext(keys, output);
            return;
        }
        if (RedisDataType.LIST.equals(dataType)) {
            pollListToNext(keys, output);
            return;
        }
        if (RedisDataType.SET.equals(dataType)) {
            pollSetToNext(keys, output);
            return;
        }
        if (RedisDataType.ZSET.equals(dataType)) {
            pollZsetToNext(keys, output);
            return;
        }
        throw new RedisConnectorException(
                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                "UnSupport redisDataType,only support string,list,hash,set,zset");
    }

    private void pollZsetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<List<String>> zSetList = redisClient.batchGetZset(keys);
        for (List<String> values : zSetList) {
            for (String value : values) {
                pollValueToNext(value, output);
            }
        }
    }

    private void pollSetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<Set<String>> setList = redisClient.batchGetSet(keys);
        for (Set<String> values : setList) {
            for (String value : values) {
                pollValueToNext(value, output);
            }
        }
    }

    private void pollListToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<List<String>> valueList = redisClient.batchGetList(keys);
        for (List<String> values : valueList) {
            for (String value : values) {
                pollValueToNext(value, output);
            }
        }
    }

    private void pollStringToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<String> values = redisClient.batchGetString(keys);
        for (String value : values) {
            pollValueToNext(value, output);
        }
    }

    private void pollValueToNext(String value, Collector<SeaTunnelRow> output) throws IOException {
        if (deserializationSchema == null) {
            output.collect(new SeaTunnelRow(new Object[] {value}));
        } else {
            deserializationSchema.deserialize(value.getBytes(), output);
        }
    }

    private void pollHashMapToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<Map<String, String>> values = redisClient.batchGetHash(keys);
        if (deserializationSchema == null) {
            for (Map<String, String> value : values) {
                output.collect(new SeaTunnelRow(new Object[] {JsonUtils.toJsonString(value)}));
            }
            return;
        }
        for (Map<String, String> recordsMap : values) {
            if (redisParameters.getHashKeyParseMode() == RedisSourceOptions.HashKeyParseMode.KV) {
                deserializationSchema.deserialize(
                        JsonUtils.toJsonString(recordsMap).getBytes(), output);
            } else {
                SeaTunnelRow seaTunnelRow =
                        new SeaTunnelRow(new Object[] {JsonUtils.toJsonString(recordsMap)});
                output.collect(seaTunnelRow);
            }
        }
    }

    private RedisDataType resolveScanType(RedisDataType dataType) {
        if (RedisDataType.KEY.equals(dataType)) {
            return RedisDataType.STRING;
        }
        return dataType;
    }
}
