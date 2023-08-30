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

import org.apache.seatunnel.connectors.seatunnel.redis.common.RedisClient;

import java.util.Collections;
import java.util.List;

public enum RedisDataType {
    KEY {
        @Override
        public void set(RedisClient redisClient, String key, String value, long expire) {
            redisClient.set(key, value, expire);
        }

        @Override
        public List<String> get(RedisClient redisClient, String key) {
            return redisClient.get(key);
        }
    },
    HASH {
        @Override
        public void set(RedisClient redisClient, String key, String value, long expire) {
            redisClient.hset(key, value, expire);
        }

        @Override
        public List<String> get(RedisClient redisClient, String key) {
            return redisClient.hget(key);
        }
    },
    LIST {
        @Override
        public void set(RedisClient redisClient, String key, String value, long expire) {
            redisClient.lpush(key, value, expire);
        }

        @Override
        public List<String> get(RedisClient redisClient, String key) {
            return redisClient.lrange(key);
        }
    },
    SET {
        @Override
        public void set(RedisClient redisClient, String key, String value, long expire) {
            redisClient.sadd(key, value, expire);
        }

        @Override
        public List<String> get(RedisClient redisClient, String key) {
            return redisClient.smembers(key);
        }
    },
    ZSET {
        @Override
        public void set(RedisClient redisClient, String key, String value, long expire) {
            redisClient.zadd(key, value, expire);
        }

        @Override
        public List<String> get(RedisClient redisClient, String key) {
            return redisClient.zrange(key);
        }
    };

    public List<String> get(RedisClient redisClient, String key) {
        return Collections.emptyList();
    }

    public void set(RedisClient redisClient, String key, String value, long expire) {
        // do nothing
    }
}
