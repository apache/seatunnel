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
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JedisSingleWrapper extends JedisWrapper {

    private final Jedis jedis;

    public JedisSingleWrapper(@NonNull Jedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public String set(final String key, final String value) {
        return jedis.set(key, value);
    }

    @Override
    public String get(final String key) {
        return jedis.get(key);
    }

    @Override
    public long hset(final String key, final Map<String, String> hash) {
        return jedis.hset(key, hash);
    }

    @Override
    public Map<String, String> hgetAll(final String key) {
        return jedis.hgetAll(key);
    }

    @Override
    public long lpush(final String key, final String... strings) {
        return jedis.lpush(key, strings);
    }

    @Override
    public List<String> lrange(final String key, final long start, final long stop) {
        return jedis.lrange(key, start, stop);
    }

    @Override
    public long sadd(final String key, final String... members) {
        return jedis.sadd(key, members);
    }

    @Override
    public Set<String> smembers(final String key) {
        return jedis.smembers(key);
    }

    @Override
    public long zadd(final String key, final double score, final String member) {
        return jedis.zadd(key, score, member);
    }

    @Override
    public List<String> zrange(final String key, final long start, final long stop) {
        return jedis.zrange(key, start, stop);
    }

    @Override
    public void close() {
        jedis.close();
    }

    @Override
    public Set<String> scanKeys(String keysPattern, String keyType, int scanCount) throws Exception {
        Set<String> matchKeys = new HashSet<>();
        ScanParams scan = new ScanParams().match(keysPattern).count(scanCount);
        ScanResult<String> scanResult = null;
        if (keyType != null) {
            scanResult = jedis.scan("0", scan, keyType);
        } else {
            scanResult = jedis.scan("0", scan);
        }
        String cursor = scanResult.getCursor();
        matchKeys.addAll(scanResult.getResult());
        while (Long.parseLong(cursor) != 0) {
            if (keyType != null) {
                scanResult = jedis.scan(cursor, scan, keyType);
            } else {
                scanResult = jedis.scan(cursor, scan);
            }
            cursor = scanResult.getCursor();
            matchKeys.addAll(scanResult.getResult());
        }
        return matchKeys;
    }
}
