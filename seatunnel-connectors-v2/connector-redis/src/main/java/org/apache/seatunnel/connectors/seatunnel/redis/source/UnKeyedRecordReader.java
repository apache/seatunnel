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
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Set;

@Slf4j
public class UnKeyedRecordReader extends RedisRecordReader {

    public UnKeyedRecordReader(
            RedisParameters redisParameters,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            RedisClient redisClient) {
        super(redisParameters, deserializationSchema, redisClient);
    }

    @Override
    public void pollZsetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<List<String>> zSetList = redisClient.batchGetZset(keys);
        for (List<String> values : zSetList) {
            for (String value : values) {
                pollValueToNext(value, output);
            }
        }
    }

    @Override
    public void pollSetToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<Set<String>> setList = redisClient.batchGetSet(keys);
        for (Set<String> values : setList) {
            for (String value : values) {
                pollValueToNext(value, output);
            }
        }
    }

    @Override
    public void pollListToNext(List<String> keys, Collector<SeaTunnelRow> output)
            throws IOException {
        List<List<String>> valueList = redisClient.batchGetList(keys);
        for (List<String> values : valueList) {
            for (String value : values) {
                pollValueToNext(value, output);
            }
        }
    }

    @Override
    public void pollStringToNext(List<String> keys, Collector<SeaTunnelRow> output)
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
}
