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

package org.apache.seatunnel.connectors.seatunnel.redis.client;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redis.config.JedisWrapper;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisErrorCode;

import redis.clients.jedis.ConnectionPoolConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;

import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Connectivity and authentication check for {@code --dry-run connect}, shared by the Redis source
 * and sink.
 *
 * <p>Only {@code AUTH}, {@code SELECT}, {@code PING}, {@code CLUSTER SLOTS} and {@code INFO} are
 * issued: no key is read, scanned, written or expired. {@link RedisParameters#buildJedis()} is
 * deliberately not reused because it calls {@code ACL SETUSER} for the {@code user} option, which
 * mutates server state.
 */
public final class RedisDryRunValidator {

    static final int TIMEOUT_MS = 10_000;
    static final int CLUSTER_MAX_ATTEMPTS = 1;

    private RedisDryRunValidator() {}

    public static void validate(RedisParameters parameters) {
        switch (parameters.getMode()) {
            case SINGLE:
                validateSingle(parameters);
                return;
            case CLUSTER:
                validateCluster(parameters);
                return;
            default:
                throw new RedisConnectorException(
                        CommonErrorCode.OPERATION_NOT_SUPPORTED, "Not support this redis mode");
        }
    }

    private static void validateSingle(RedisParameters parameters) {
        String target = parameters.getHost() + ":" + parameters.getPort();
        try (Jedis jedis =
                new Jedis(parameters.getHost(), parameters.getPort(), TIMEOUT_MS, TIMEOUT_MS)) {
            if (StringUtils.isNotBlank(parameters.getAuth())) {
                jedis.auth(parameters.getAuth());
            }
            jedis.select(parameters.getDbNum());
            jedis.ping();
        } catch (RuntimeException e) {
            throw connectionError(target, e);
        }
    }

    private static void validateCluster(RedisParameters parameters) {
        Set<HostAndPort> nodes = new LinkedHashSet<>();
        for (String redisNode : parameters.getRedisNodes()) {
            String[] splits = redisNode.split(":");
            nodes.add(new HostAndPort(splits[0], Integer.parseInt(splits[1])));
        }
        String target = String.valueOf(parameters.getRedisNodes());
        String password =
                StringUtils.isNotBlank(parameters.getAuth()) ? parameters.getAuth() : null;
        try {
            // The constructor initializes the slot cache, which already proves that at least one
            // node is reachable and accepts the credentials.
            JedisCluster jedisCluster =
                    new JedisCluster(
                            nodes,
                            TIMEOUT_MS,
                            TIMEOUT_MS,
                            CLUSTER_MAX_ATTEMPTS,
                            password,
                            new ConnectionPoolConfig());
            // Closing the wrapper also closes the cluster client and its node connections.
            try (JedisWrapper jedisWrapper = new JedisWrapper(jedisCluster)) {
                jedisWrapper.info();
            }
        } catch (RuntimeException e) {
            throw connectionError(target, e);
        }
    }

    private static RedisConnectorException connectionError(String target, Throwable cause) {
        return new RedisConnectorException(
                RedisErrorCode.REDIS_CONNECTION_ERROR,
                String.format(
                        "Redis connect dry-run failed for %s: %s", target, cause.getMessage()),
                cause);
    }
}
