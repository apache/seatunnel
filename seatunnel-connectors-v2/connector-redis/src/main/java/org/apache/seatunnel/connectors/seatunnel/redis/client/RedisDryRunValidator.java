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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.redis.config.RedisParameters;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisConnectorException;
import org.apache.seatunnel.connectors.seatunnel.redis.exception.RedisErrorCode;

import redis.clients.jedis.Jedis;

/**
 * Connectivity and authentication check for {@code --dry-run connect}, shared by the Redis source
 * and sink.
 *
 * <p>The client is created through {@link RedisParameters#buildJedis()}, the same connection setup
 * the runtime uses, so the {@code user} and {@code auth} options are verified exactly as they are
 * during job execution. Afterwards only {@code PING} (single node) or {@code INFO} (cluster) is
 * issued: no key is read, scanned, written or expired, no key space is created and no ACL is
 * modified. The client is closed on success and on failure.
 */
public final class RedisDryRunValidator {

    private RedisDryRunValidator() {}

    /**
     * Opens one short-lived connection with the runtime connection setup and closes it again.
     *
     * @param parameters connection parameters built from the source or sink options
     * @throws RedisConnectorException with {@link RedisErrorCode#REDIS_CONNECTION_ERROR} naming the
     *     validated target when Redis is unreachable or rejects the credentials
     */
    public static void validate(RedisParameters parameters) {
        String target = target(parameters);
        Jedis jedis;
        try {
            // Sends AUTH (and SELECT in SINGLE mode, CLUSTER SLOTS in CLUSTER mode) and already
            // closes the connection itself when that fails.
            jedis = parameters.buildJedis();
        } catch (RuntimeException e) {
            throw connectionError(target, e);
        }
        try (Jedis client = jedis) {
            switch (parameters.getMode()) {
                case SINGLE:
                    client.ping();
                    break;
                case CLUSTER:
                    // JedisWrapper has no connection of its own, so INFO is read from a node.
                    client.info();
                    break;
                default:
                    throw unsupportedMode();
            }
        } catch (RuntimeException e) {
            throw connectionError(target, e);
        }
    }

    private static String target(RedisParameters parameters) {
        switch (parameters.getMode()) {
            case SINGLE:
                return parameters.getHost() + ":" + parameters.getPort();
            case CLUSTER:
                return String.valueOf(parameters.getRedisNodes());
            default:
                throw unsupportedMode();
        }
    }

    private static RedisConnectorException unsupportedMode() {
        return new RedisConnectorException(
                CommonErrorCode.OPERATION_NOT_SUPPORTED, "Not support this redis mode");
    }

    private static RedisConnectorException connectionError(String target, Throwable cause) {
        return new RedisConnectorException(
                RedisErrorCode.REDIS_CONNECTION_ERROR,
                String.format(
                        "Redis connect dry-run failed for %s: %s", target, cause.getMessage()),
                cause);
    }
}
