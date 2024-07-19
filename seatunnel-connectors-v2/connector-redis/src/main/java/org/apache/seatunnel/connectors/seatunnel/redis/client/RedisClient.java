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

package org.apache.seatunnel.connectors.seatunnel.redis.client;

import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisDataType;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class RedisClient extends Jedis {

    protected final RedisParameters redisParameters;

    protected final int batchSize;

    protected final Jedis jedis;

    protected RedisClient(RedisParameters redisParameters, Jedis jedis) {
        this.redisParameters = redisParameters;
        this.batchSize = redisParameters.getBatchSize();
        this.jedis = jedis;
    }

    public ScanResult<String> scanKeys(
            String cursor, int batchSize, String keysPattern, RedisDataType type) {
        ScanParams scanParams = new ScanParams();
        scanParams.match(keysPattern);
        scanParams.count(batchSize);
        return jedis.scan(cursor, scanParams, type.name());
    }

    public abstract List<String> batchGetString(List<String> keys);

    public abstract List<List<String>> batchGetList(List<String> keys);

    public abstract List<Set<String>> batchGetSet(List<String> keys);

    public abstract List<Map<String, String>> batchGetHash(List<String> keys);

    public abstract List<List<String>> batchGetZset(List<String> keys);

    public abstract void batchWriteString(
            List<String> keys, List<String> values, long expireSeconds);

    public abstract void batchWriteList(
            List<String> keyBuffer, List<String> valueBuffer, long expireSeconds);

    public abstract void batchWriteSet(
            List<String> keyBuffer, List<String> valueBuffer, long expireSeconds);

    public abstract void batchWriteHash(
            List<String> keyBuffer, List<String> valueBuffer, long expireSeconds);

    public abstract void batchWriteZset(
            List<String> keyBuffer, List<String> valueBuffer, long expireSeconds);
}
