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

import redis.clients.jedis.Jedis;

import java.util.Set;

public abstract class JedisWrapper extends Jedis {

    protected abstract Set<String> scanKeys(String keysPattern, String keyType, int scanCount)  throws Exception;

    public Set<String> scanKeys(RedisParameters redisParameters)  throws Exception {
        // if true, check keys type
        String keysPattern = redisParameters.getKeysPattern();
        int scanCount = redisParameters.getScanCount();
        if (redisParameters.isKeysTypeCheck()) {
            String keyType = redisParameters.getRedisDataType().name();
            if (keyType.equals(RedisDataType.KEY.name())) {
                keyType = "STRING";
            }
            return scanKeys(keysPattern, keyType, scanCount);
        } else {
            return scanKeys(keysPattern, null, scanCount);
        }
    }

}
