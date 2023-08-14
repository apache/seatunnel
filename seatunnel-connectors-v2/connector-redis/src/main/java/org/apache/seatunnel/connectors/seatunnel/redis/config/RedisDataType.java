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
import redis.clients.jedis.JedisCluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public enum RedisDataType {
    KEY {
        @Override
        public void set(Jedis jedis, JedisCluster jedisCluster, String key, String value, long expire, RedisConfig.RedisMode redisMode) {
            if (RedisConfig.RedisMode.SINGLE == redisMode) {
                jedis.set(key, value);
            } else {
                jedisCluster.set(key, value);
            }
            expire(jedis, jedisCluster, key, expire, redisMode);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            return Collections.singletonList(jedis.get(key));
        }
    },
    HASH {
        @Override
        public void set(Jedis jedis, JedisCluster jedisCluster, String key, String value, long expire, RedisConfig.RedisMode redisMode) {
            Map<String, String> fieldsMap = JsonUtils.toMap(value);
            if (RedisConfig.RedisMode.SINGLE == redisMode) {
                jedis.hset(key, fieldsMap);
            }else {
                jedisCluster.hset(key, fieldsMap);
            }
            expire(jedis, jedisCluster, key, expire, redisMode);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            Map<String, String> kvMap = jedis.hgetAll(key);
            return Collections.singletonList(JsonUtils.toJsonString(kvMap));
        }
    },
    LIST {
        @Override
        public void set(Jedis jedis, JedisCluster jedisCluster, String key, String value, long expire, RedisConfig.RedisMode redisMode) {
            if (RedisConfig.RedisMode.SINGLE == redisMode) {
                jedis.lpush(key, value);
            }else{
                jedisCluster.lpush(key, value);
            }
            expire(jedis, jedisCluster, key, expire, redisMode);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            return jedis.lrange(key, 0, -1);
        }
    },
    SET {
        @Override
        public void set(Jedis jedis, JedisCluster jedisCluster, String key, String value, long expire, RedisConfig.RedisMode redisMode) {
            if (RedisConfig.RedisMode.SINGLE == redisMode) {
                jedis.sadd(key, value);
            }else{
                jedisCluster.sadd(key, value);
            }
            expire(jedis, jedisCluster, key, expire, redisMode);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            Set<String> members = jedis.smembers(key);
            return new ArrayList<>(members);
        }
    },
    ZSET {
        @Override
        public void set(Jedis jedis, JedisCluster jedisCluster, String key, String value, long expire, RedisConfig.RedisMode redisMode) {
            if (RedisConfig.RedisMode.SINGLE == redisMode) {
                jedis.zadd(key, 1, value);
            }else{
                jedisCluster.zadd(key, 1, value);
            }
            expire(jedis, jedisCluster, key, expire, redisMode);
        }

        @Override
        public List<String> get(Jedis jedis, String key) {
            return jedis.zrange(key, 0, -1);
        }
    };

    public List<String> get(Jedis jedis, String key) {
        return Collections.emptyList();
    }

    private static void expire(Jedis jedis,JedisCluster jedisCluster, String key, long expire, RedisConfig.RedisMode redisMode) {
        if (expire > 0) {
            if (RedisConfig.RedisMode.SINGLE == redisMode) {
                jedis.expire(key, expire);
            } else {
                jedisCluster.expire(key, expire);
            }

        }
    }

    public void set(Jedis jedis, JedisCluster jedisCluster, String key, String value, long expire, RedisConfig.RedisMode redisMode) {
        // do nothing
    }
}
