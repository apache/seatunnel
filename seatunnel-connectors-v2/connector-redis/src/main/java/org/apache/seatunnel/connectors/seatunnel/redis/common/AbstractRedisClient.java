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

package org.apache.seatunnel.connectors.seatunnel.redis.common;

import org.apache.seatunnel.common.utils.JsonUtils;

import redis.clients.jedis.commands.JedisCommands;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class AbstractRedisClient implements RedisClient {
    protected JedisCommands jedisCommands;

    public AbstractRedisClient(JedisCommands jedisCommands) {
        this.jedisCommands = jedisCommands;
    }

    @Override
    public void set(String key, String value, long expire) {
        jedisCommands.set(key, value);
        if (expire > 0) {
            expire(key, expire);
        }
    }

    @Override
    public List<String> get(String key) {
        return Collections.singletonList(jedisCommands.get(key));
    }

    @Override
    public void hset(String key, String value, long expire) {
        Map<String, String> fieldsMap = JsonUtils.toMap(value);
        jedisCommands.hset(key, fieldsMap);
        if (expire > 0) {
            expire(key, expire);
        }
    }

    @Override
    public List<String> hget(String key) {
        Map<String, String> kvMap = jedisCommands.hgetAll(key);
        return Collections.singletonList(JsonUtils.toJsonString(kvMap));
    }

    @Override
    public void lpush(String key, String value, long expire) {
        jedisCommands.lpush(key, value);
        if (expire > 0) {
            expire(key, expire);
        }
    }

    @Override
    public List<String> lrange(String key) {
        return jedisCommands.lrange(key, 0, -1);
    }

    @Override
    public void sadd(String key, String value, long expire) {
        jedisCommands.sadd(key, value);
        if (expire > 0) {
            expire(key, expire);
        }
    }

    @Override
    public List<String> smembers(String key) {
        Set<String> members = jedisCommands.smembers(key);
        return new ArrayList<>(members);
    }

    @Override
    public void zadd(String key, String value, long expire) {
        jedisCommands.zadd(key, 1, value);
        if (expire > 0) {
            expire(key, expire);
        }
    }

    @Override
    public List<String> zrange(String key) {
        return jedisCommands.zrange(key, 0, -1);
    }

    @Override
    public void expire(String key, long seconds) {
        jedisCommands.expire(key, seconds);
    }

    @Override
    public Set<String> keys(String keys) {
        return jedisCommands.keys(keys);
    }

    public abstract void close();
}
