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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

@Data
public class RedisParameters implements Serializable {
    private String host;
    private int port;
    private String auth = "";
    private String user = "";
    private String keysPattern;
    private String keyField;
    private RedisDataType redisDataType;
    private RedisConfig.RedisMode mode;
    private RedisConfig.HashKeyParseMode hashKeyParseMode;
    private List<String> redisNodes = Collections.emptyList();

    public void buildWithConfig(Config config) {
        // set host
        this.host = config.getString(RedisConfig.HOST.key());
        // set port
        this.port = config.getInt(RedisConfig.PORT.key());
        // set auth
        if (config.hasPath(RedisConfig.AUTH.key())) {
            this.auth = config.getString(RedisConfig.AUTH.key());
        }
        // set user
        if (config.hasPath(RedisConfig.USER.key())) {
            this.user = config.getString(RedisConfig.USER.key());
        }
        // set mode
        if (config.hasPath(RedisConfig.MODE.key())) {
            this.mode = RedisConfig.RedisMode
                    .valueOf(config.getString(RedisConfig.MODE.key()).toUpperCase());
        } else {
            this.mode = RedisConfig.MODE.defaultValue();
        }
        // set hash key mode
        if (config.hasPath(RedisConfig.HASH_KEY_PARSE_MODE.key())) {
            this.hashKeyParseMode = RedisConfig.HashKeyParseMode
                    .valueOf(config.getString(RedisConfig.HASH_KEY_PARSE_MODE.key()).toUpperCase());
        } else {
            this.hashKeyParseMode = RedisConfig.HASH_KEY_PARSE_MODE.defaultValue();
        }
        // set redis nodes information
        if (config.hasPath(RedisConfig.NODES.key())) {
            this.redisNodes = config.getStringList(RedisConfig.NODES.key());
        }
        // set key
        if (config.hasPath(RedisConfig.KEY.key())) {
            this.keyField = config.getString(RedisConfig.KEY.key());
        }
        // set keysPattern
        if (config.hasPath(RedisConfig.KEY_PATTERN.key())) {
            this.keysPattern = config.getString(RedisConfig.KEY_PATTERN.key());
        }
        // set redis data type
        try {
            String dataType = config.getString(RedisConfig.DATA_TYPE.key());
            this.redisDataType = RedisDataType.valueOf(dataType.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new RedisConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Redis source connector only support these data types [key, hash, list, set, zset]", e);
        }
    }

    public Jedis buildJedis() {
        switch (mode) {
            case SINGLE:
                Jedis jedis = new Jedis(host, port);
                if (StringUtils.isNotBlank(auth)) {
                    jedis.auth(auth);
                }
                if (StringUtils.isNotBlank(user)) {
                    jedis.aclSetUser(user);
                }
                return jedis;
            case CLUSTER:
                HashSet<HostAndPort> nodes = new HashSet<>();
                HostAndPort node = new HostAndPort(host, port);
                nodes.add(node);
                if (!redisNodes.isEmpty()) {
                    for (String redisNode : redisNodes) {
                        String[] splits = redisNode.split(":");
                        if (splits.length != 2) {
                            throw new RedisConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                                    "Invalid redis node information," +
                                    "redis node information must like as the following: [host:port]");
                        }
                        HostAndPort hostAndPort = new HostAndPort(splits[0], Integer.parseInt(splits[1]));
                        nodes.add(hostAndPort);
                    }
                }
                ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();
                JedisCluster jedisCluster;
                if (StringUtils.isNotBlank(auth)) {
                    jedisCluster = new JedisCluster(nodes, JedisCluster.DEFAULT_TIMEOUT,
                            JedisCluster.DEFAULT_TIMEOUT, JedisCluster.DEFAULT_MAX_ATTEMPTS,
                            auth, connectionPoolConfig);
                } else {
                    jedisCluster = new JedisCluster(nodes);
                }
                return new JedisWrapper(jedisCluster);
            default:
                // do nothing
                throw new RedisConnectorException(CommonErrorCode.UNSUPPORTED_OPERATION,
                        "Not support this redis mode");
        }
    }
}
