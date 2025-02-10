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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClient;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisClusterClient;
import org.apache.seatunnel.connectors.seatunnel.redis.client.RedisSingleClient;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisErrorCode.GET_REDIS_VERSION_INFO_FAILED;
import static org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisErrorCode.INVALID_CONFIG;

@Data
@Slf4j
public class RedisParameters implements Serializable {
    private String host;
    private int port;
    private String auth = "";
    private int dbNum;
    private String user = "";
    private String keysPattern;
    private String keyField;
    private RedisDataType redisDataType;
    private RedisBaseOptions.RedisMode mode;
    private RedisSourceOptions.HashKeyParseMode hashKeyParseMode;
    private List<String> redisNodes = Collections.emptyList();
    private long expire = RedisSinkOptions.EXPIRE.defaultValue();
    private int batchSize = RedisBaseOptions.BATCH_SIZE.defaultValue();
    private Boolean supportCustomKey;
    private String valueField;
    private String hashKeyField;
    private String hashValueField;

    private int redisVersion;

    public void buildWithConfig(ReadonlyConfig config) {
        // set host
        this.host = config.get(RedisBaseOptions.HOST);
        // set port
        this.port = config.get(RedisBaseOptions.PORT);
        // set db_num
        this.dbNum = config.get(RedisBaseOptions.DB_NUM);
        // set hash key mode
        this.hashKeyParseMode = config.get(RedisSourceOptions.HASH_KEY_PARSE_MODE);
        // set expire
        this.expire = config.get(RedisSinkOptions.EXPIRE);
        // set auth
        if (config.getOptional(RedisBaseOptions.AUTH).isPresent()) {
            this.auth = config.get(RedisBaseOptions.AUTH);
        }
        // set user
        if (config.getOptional(RedisBaseOptions.USER).isPresent()) {
            this.user = config.get(RedisBaseOptions.USER);
        }
        // set mode
        this.mode = config.get(RedisBaseOptions.MODE);
        // set redis nodes information
        if (config.getOptional(RedisBaseOptions.NODES).isPresent()) {
            this.redisNodes = config.get(RedisBaseOptions.NODES);
        }
        // set key
        if (config.getOptional(RedisBaseOptions.KEY).isPresent()) {
            this.keyField = config.get(RedisBaseOptions.KEY);
        }
        // set keysPattern
        if (config.getOptional(RedisBaseOptions.KEY_PATTERN).isPresent()) {
            this.keysPattern = config.get(RedisBaseOptions.KEY_PATTERN);
        }
        // set redis data type verification factory createAndPrepareSource
        this.redisDataType = config.get(RedisBaseOptions.DATA_TYPE);
        // Indicates the number of keys to attempt to return per iteration.default 10
        this.batchSize = config.get(RedisBaseOptions.BATCH_SIZE);
        // set support custom key
        if (config.getOptional(RedisSinkOptions.SUPPORT_CUSTOM_KEY).isPresent()) {
            this.supportCustomKey = config.get(RedisSinkOptions.SUPPORT_CUSTOM_KEY);
        }
        // set value field
        if (config.getOptional(RedisSinkOptions.VALUE_FIELD).isPresent()) {
            this.valueField = config.get(RedisSinkOptions.VALUE_FIELD);
        }
        // set hash key field
        if (config.getOptional(RedisSinkOptions.HASH_KEY_FIELD).isPresent()) {
            this.hashKeyField = config.get(RedisSinkOptions.HASH_KEY_FIELD);
        }
        // set hash value field
        if (config.getOptional(RedisSinkOptions.HASH_VALUE_FIELD).isPresent()) {
            this.hashValueField = config.get(RedisSinkOptions.HASH_VALUE_FIELD);
        }
    }

    public RedisClient buildRedisClient() {
        Jedis jedis = this.buildJedis();
        this.redisVersion = extractRedisVersion(jedis);
        if (mode.equals(RedisBaseOptions.RedisMode.SINGLE)) {
            return new RedisSingleClient(this, jedis, redisVersion);
        } else {
            return new RedisClusterClient(this, jedis, redisVersion);
        }
    }

    private int extractRedisVersion(Jedis jedis) {
        log.info("Try to get redis version information from the jedis.info() method");
        // # Server
        // redis_version:5.0.14
        // redis_git_sha1:00000000
        // redis_git_dirty:0
        String info = jedis.info();
        try {
            for (String line : info.split("\n")) {
                if (line.startsWith("redis_version:")) {
                    // 5.0.14
                    String versionInfo = line.split(":")[1].trim();
                    log.info("The version of Redis is :{}", versionInfo);
                    String[] parts = versionInfo.split("\\.");
                    return Integer.parseInt(parts[0]);
                }
            }
        } catch (Exception e) {
            throw new RedisConnectorException(
                    GET_REDIS_VERSION_INFO_FAILED,
                    GET_REDIS_VERSION_INFO_FAILED.getErrorMessage(),
                    e);
        }
        throw new RedisConnectorException(
                GET_REDIS_VERSION_INFO_FAILED,
                "Did not get the expected redis_version from the jedis.info() method");
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
                jedis.select(dbNum);
                return jedis;
            case CLUSTER:
                HashSet<HostAndPort> nodes = new HashSet<>();
                HostAndPort node = new HostAndPort(host, port);
                nodes.add(node);
                if (!redisNodes.isEmpty()) {
                    for (String redisNode : redisNodes) {
                        String[] splits = redisNode.split(":");
                        if (splits.length != 2) {
                            throw new RedisConnectorException(
                                    INVALID_CONFIG,
                                    "Invalid redis node information,"
                                            + "redis node information must like as the following: [host:port]");
                        }
                        HostAndPort hostAndPort =
                                new HostAndPort(splits[0], Integer.parseInt(splits[1]));
                        nodes.add(hostAndPort);
                    }
                }
                ConnectionPoolConfig connectionPoolConfig = new ConnectionPoolConfig();
                JedisCluster jedisCluster;
                if (StringUtils.isNotBlank(auth)) {
                    jedisCluster =
                            new JedisCluster(
                                    nodes,
                                    JedisCluster.DEFAULT_TIMEOUT,
                                    JedisCluster.DEFAULT_TIMEOUT,
                                    JedisCluster.DEFAULT_MAX_ATTEMPTS,
                                    auth,
                                    connectionPoolConfig);
                } else {
                    jedisCluster = new JedisCluster(nodes);
                }
                JedisWrapper jedisWrapper = new JedisWrapper(jedisCluster);
                jedisWrapper.select(dbNum);
                return jedisWrapper;
            default:
                // do nothing
                throw new RedisConnectorException(
                        CommonErrorCode.OPERATION_NOT_SUPPORTED, "Not support this redis mode");
        }
    }
}
