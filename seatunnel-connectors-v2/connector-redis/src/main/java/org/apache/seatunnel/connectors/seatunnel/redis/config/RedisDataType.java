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

import org.apache.seatunnel.common.utils.JsonUtils;

import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public enum RedisDataType {
    KEY {
        @Override
        public void set(Jedis jedis, String key, String value) {
            jedis.set(key, value);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            return Collections.singletonList(jedis.get(key));
        }
    },
    HASH {
        @Override
        public void set(Jedis jedis, String key, String value) {
            Map<String, String> fieldsMap = JsonUtils.toMap(value);
            jedis.hset(key, fieldsMap);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            Map<String, String> kvMap = jedis.hgetAll(key);
            return Collections.singletonList(JsonUtils.toJsonString(kvMap));
        }

    },
    LIST {
        @Override
        public void set(Jedis jedis, String key, String value) {
            jedis.lpush(key, value);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            return jedis.lrange(key, 0, -1);
        }
    },
    SET {
        @Override
        public void set(Jedis jedis, String key, String value) {
            jedis.sadd(key, value);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            Set<String> members = jedis.smembers(key);
            return new ArrayList<>(members);
        }
    },
    ZSET {
        @Override
        public void set(Jedis jedis, String key, String value) {
            jedis.zadd(key, 1, value);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            return jedis.zrange(key, 0, -1);
        }
    };

    public List<String> get(Jedis jedis, String key) {
        return Collections.emptyList();
    }

    public void set(Jedis jedis, String key, String value) {
        // do nothing
    }
}
