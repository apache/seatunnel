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

package org.apache.seatunnel.connectors.seatunnel.redis.config;

import lombok.NonNull;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisWrapper extends Jedis {
    private final JedisCluster jedisCluster;

    public JedisWrapper(@NonNull JedisCluster jedisCluster) {
        this.jedisCluster = jedisCluster;
    }

    @Override
    public String set(final String key, final String value) {
        return jedisCluster.set(key, value);
    }

    @Override
    public String get(final String key) {
        return jedisCluster.get(key);
    }

    @Override
    public long hset(final String key, final Map<String, String> hash) {
        return jedisCluster.hset(key, hash);
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        return jedisCluster.hgetAll(key);
    }

    @Override
    public long lpush(final String key, final String... strings) {
        return jedisCluster.lpush(key, strings);
    }

    @Override
    public List<String> lrange(final String key, final long start, final long stop) {
        return jedisCluster.lrange(key, start, stop);
    }

    @Override
    public long sadd(final String key, final String... members) {
        return jedisCluster.sadd(key, members);
    }

    @Override
    public Set<String> smembers(final String key) {
        return jedisCluster.smembers(key);
    }

    @Override
    public long zadd(final String key, final double score, final String member) {
        return jedisCluster.zadd(key, score, member);
    }

    @Override
    public List<String> zrange(final String key, final long start, final long stop) {
        return jedisCluster.zrange(key, start, stop);
    }

    @Override
    public void close() {
        jedisCluster.close();
    }
}
