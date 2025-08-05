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
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.util.KeyValueMerger;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Slf4j
public class KeyedRecordReader extends RedisRecordReader {

    private final KeyValueMerger keyValueMerger;

    public KeyedRecordReader(
            RedisParameters redisParameters,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            RedisClient redisClient,
            KeyValueMerger keyValueMerger) {
        super(redisParameters, deserializationSchema, redisClient);
        this.keyValueMerger = keyValueMerger;
    }

    @Override
    public void pollZsetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<List<String>> zSetList = redisClient.batchGetZset(keys);
        for (int i = 0; i < zSetList.size(); i++) {
            for (String value : zSetList.get(i)) {
                pollValueToNext(keys.get(i), value, output);
            }
        }
    }

    @Override
    public void pollSetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<Set<String>> setList = redisClient.batchGetSet(keys);
        for (int i = 0; i < setList.size(); i++) {
            for (String value : setList.get(i)) {
                pollValueToNext(keys.get(i), value, output);
            }
        }
    }

    @Override
    public void pollListToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<List<String>> valueList = redisClient.batchGetList(keys);
        for (int i = 0; i < valueList.size(); i++) {
            for (String value : valueList.get(i)) {
                pollValueToNext(keys.get(i), value, output);
            }
        }
    }

    @Override
    public void pollStringToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<String> values = redisClient.batchGetString(keys);
        for (int i = 0; i < values.size(); i++) {
            pollValueToNext(keys.get(i), values.get(i), output);
        }
    }

    private void pollValueToNext(String key, String value, Collector<SeaTunnelRow> output)
            throws IOException {
        if (deserializationSchema == null) {
            throw CommonError.illegalArgument(
                    "deserializationSchema is null",
                    "Redis source requires a deserialization schema to parse the record with key: "
                            + key);
        } else {
            String parsed = keyValueMerger.parseWithKey(key, value);
            deserializationSchema.deserialize(parsed.getBytes(), output);
        }
    }
}
