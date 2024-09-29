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

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;

import org.apache.commons.collections4.CollectionUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

// In standalone mode, pipeline can be used to improve batch read performance
public class RedisSingleClient extends RedisClient {

    public RedisSingleClient(RedisParameters redisParameters, Jedis jedis, int redisVersion) {
        super(redisParameters, jedis, redisVersion);
    }

    @Override
    public List<String> batchGetString(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        String[] keyArr = keys.toArray(new String[0]);
        return jedis.mget(keyArr);
    }

    @Override
    public List<List<String>> batchGetList(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Response<List<String>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            responses.add(pipeline.lrange(key, 0, -1));
        }

        pipeline.sync();

        List<List<String>> resultList = new ArrayList<>(keys.size());
        for (Response<List<String>> response : responses) {
            resultList.add(response.get());
        }

        return resultList;
    }

    @Override
    public List<Set<String>> batchGetSet(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Response<Set<String>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            responses.add(pipeline.smembers(key));
        }

        pipeline.sync();

        List<Set<String>> resultList = new ArrayList<>(keys.size());
        for (Response<Set<String>> response : responses) {
            resultList.add(response.get());
        }

        return resultList;
    }

    @Override
    public List<Map<String, String>> batchGetHash(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        Pipeline pipeline = jedis.pipelined();
        List<Response<Map<String, String>>> responses = new ArrayList<>(keys.size());

        for (String key : keys) {
            Response<Map<String, String>> response = pipeline.hgetAll(key);
            responses.add(response);
        }

        pipeline.sync();

        List<Map<String, String>> resultList = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            Response<Map<String, String>> response = responses.get(i);
            Map<String, String> map = response.get();
            if (map != null) {
                map.put("hash_key", keys.get(i));
            }
            resultList.add(map);
        }

        return resultList;
    }

    @Override
    public List<List<String>> batchGetZset(List<String> keys) {
        if (CollectionUtils.isEmpty(keys)) {
            return new ArrayList<>();
        }
        List<Response<List<String>>> responses = new ArrayList<>(keys.size());
        Pipeline pipelined = jedis.pipelined();
        for (String key : keys) {
            Response<List<String>> response = pipelined.zrange(key, 0, -1);
            responses.add(response);
        }
        pipelined.sync();
        List<List<String>> resultlist = new ArrayList<>(keys.size());
        for (Response<List<String>> response : responses) {
            resultlist.add(response.get());
        }
        return resultlist;
    }

    @Override
    public void batchWriteString(List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            String key = keys.get(i);
            String value = values.get(i);
            pipelined.set(key, value);
            if (expireSeconds > 0) {
                pipelined.expire(key, expireSeconds);
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteList(List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            String key = keys.get(i);
            String value = values.get(i);
            pipelined.lpush(key, value);
            if (expireSeconds > 0) {
                pipelined.expire(key, expireSeconds);
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteSet(List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            String key = keys.get(i);
            String value = values.get(i);
            pipelined.sadd(key, value);
            if (expireSeconds > 0) {
                pipelined.expire(key, expireSeconds);
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteHash(List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            String key = keys.get(i);
            String value = values.get(i);
            Map<String, String> fieldsMap = JsonUtils.toMap(value);
            pipelined.hset(key, fieldsMap);
            if (expireSeconds > 0) {
                pipelined.expire(key, expireSeconds);
            }
        }
        pipelined.sync();
    }

    @Override
    public void batchWriteZset(List<String> keys, List<String> values, long expireSeconds) {
        Pipeline pipelined = jedis.pipelined();
        int size = keys.size();
        for (int i = 0; i < size; i++) {
            String key = keys.get(i);
            String value = values.get(i);
            pipelined.zadd(key, 1, value);
            if (expireSeconds > 0) {
                pipelined.expire(key, expireSeconds);
            }
        }
        pipelined.sync();
    }
}
