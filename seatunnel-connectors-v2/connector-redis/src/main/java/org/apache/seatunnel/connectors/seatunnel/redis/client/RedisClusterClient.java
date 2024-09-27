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

import org.apache.commons.collections4.CollectionUtils;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RedisClusterClient extends RedisClient {
    public RedisClusterClient(RedisParameters redisParameters, Jedis jedis, int redisVersion) {
        super(redisParameters, jedis, redisVersion);
    }

    @Override
    public List<String> batchGetString(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<String> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.get(key));
        }
        return result;
    }

    @Override
    public List<List<String>> batchGetList(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<List<String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.lrange(key, 0, -1));
        }
        return result;
    }

    @Override
    public List<Set<String>> batchGetSet(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Set<String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.smembers(key));
        }
        return result;
    }

    @Override
    public List<Map<String, String>> batchGetHash(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Map<String, String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            Map<String, String> map = jedis.hgetAll(key);
            map.put("hash_key", key);
            result.add(map);
        }
        return result;
    }

    @Override
    public List<List<String>> batchGetZset(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<List<String>> result = new ArrayList<>(keys.size());
        for (String key : keys) {
            result.add(jedis.zrange(key, 0, -1));
        }
        return result;
    }

    @Override
    public void batchWriteString(List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RedisDataType.STRING.set(this, keys.get(i), values.get(i), expireSeconds);
        }
    }

    @Override
    public void batchWriteList(List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RedisDataType.LIST.set(this, keys.get(i), values.get(i), expireSeconds);
        }
    }

    @Override
    public void batchWriteSet(List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RedisDataType.SET.set(this, keys.get(i), values.get(i), expireSeconds);
        }
    }

    @Override
    public void batchWriteHash(List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RedisDataType.HASH.set(this, keys.get(i), values.get(i), expireSeconds);
        }
    }

    @Override
    public void batchWriteZset(List<String> keys, List<String> values, long expireSeconds) {
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            RedisDataType.ZSET.set(this, keys.get(i), values.get(i), expireSeconds);
        }
    }
}
