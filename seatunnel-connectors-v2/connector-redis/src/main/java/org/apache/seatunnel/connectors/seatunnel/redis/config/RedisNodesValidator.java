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
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import java.util.List;
import java.util.regex.Pattern;

public class RedisNodesValidator implements ConditionExtension<List<String>> {

    private static final Pattern NODE_PATTERN = Pattern.compile("^[^:]+:\\d+$");

    @Override
    public String description() {
        return "each node must be in 'host:port' format with port in ["
                + RedisBaseOptions.MIN_PORT
                + ", "
                + RedisBaseOptions.MAX_PORT
                + "]";
    }

    @Override
    public boolean evaluate(ReadonlyConfig config, List<String> value)
            throws OptionValidationException {
        for (int i = 0; i < value.size(); i++) {
            String node = value.get(i);
            if (node == null || !NODE_PATTERN.matcher(node).matches()) {
                throw new OptionValidationException(
                        "nodes[%d]: must be in 'host:port' format, got: %s", i, node);
            }
            int colon = node.indexOf(':');
            String host = node.substring(0, colon);
            if (host.trim().isEmpty()) {
                throw new OptionValidationException(
                        "nodes[%d]: host must not be blank in 'host:port', got: %s", i, node);
            }
            String portStr = node.substring(colon + 1);
            int port;
            try {
                port = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                throw new OptionValidationException(
                        "nodes[%d]: port '%s' is out of range [%d, %d]",
                        i, portStr, RedisBaseOptions.MIN_PORT, RedisBaseOptions.MAX_PORT);
            }
            if (port < RedisBaseOptions.MIN_PORT || port > RedisBaseOptions.MAX_PORT) {
                throw new OptionValidationException(
                        "nodes[%d]: port %d is out of range [%d, %d]",
                        i, port, RedisBaseOptions.MIN_PORT, RedisBaseOptions.MAX_PORT);
            }
        }
        return true;
    }
}
