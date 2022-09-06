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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.Jedis;

import java.io.Serializable;

@Data
public class RedisParameters implements Serializable {
    private String host;
    private int port;
    private String auth = "";
    private String keysPattern;
    private String keyField;
    private RedisDataType redisDataType;

    public void buildWithConfig(Config config) {
        // set host
        this.host = config.getString(RedisConfig.HOST);
        // set port
        this.port = config.getInt(RedisConfig.PORT);
        // set auth
        if (config.hasPath(RedisConfig.AUTH)) {
            this.auth = config.getString(RedisConfig.AUTH);
        }
        // set keyField
        if (config.hasPath(RedisConfig.KEY_FIELD)) {
            this.keyField = config.getString(RedisConfig.KEY_FIELD);
        }
        // set keysPattern
        if (config.hasPath(RedisConfig.KEY_PATTERN)) {
            this.keysPattern = config.getString(RedisConfig.KEY_PATTERN);
        }
        // set redis data type
        try {
            String dataType = config.getString(RedisConfig.DATA_TYPE);
            this.redisDataType = RedisDataType.valueOf(dataType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("Redis source connector only support these data types [key, hash, list, set, zset]", e);
        }
    }

    public Jedis buildJedis() {
        Jedis jedis = new Jedis(host, port);
        if (StringUtils.isNotBlank(auth)) {
            jedis.auth(auth);
        }
        return jedis;
    }
}
