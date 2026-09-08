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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.client;

import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import com.vesoft.nebula.client.graph.SessionPool;
import com.vesoft.nebula.client.graph.SessionPoolConfig;
import com.vesoft.nebula.client.graph.data.ResultSet;

import java.io.IOException;
import java.util.Map;

public final class SessionPoolNebulaGraphClient implements NebulaGraphClient {

    private final SessionPool sessionPool;

    public SessionPoolNebulaGraphClient(NebulaGraphSinkConfig config) {
        SessionPoolConfig poolConfig =
                new SessionPoolConfig(
                                config.getHosts(),
                                config.getSpace(),
                                config.getUsername(),
                                config.getPassword())
                        .setMinSessionSize(1)
                        .setMaxSessionSize(1)
                        .setTimeout(config.getTimeoutMillis())
                        .setWaitTime(config.getTimeoutMillis())
                        .setRetryTimes(config.getMaxRetries())
                        .setIntervalTime(
                                config.getMaxRetries() == 0 ? 0 : config.getRetryIntervalMillis())
                        .setReconnect(true);
        try {
            this.sessionPool = new SessionPool(poolConfig);
        } catch (RuntimeException e) {
            throw new NebulaGraphConnectorException(
                    NebulaGraphConnectorErrorCode.CONNECT_FAILED,
                    "Unable to initialize a NebulaGraph session pool for space '"
                            + config.getSpace()
                            + "'.",
                    e);
        }
    }

    @Override
    public void execute(String statement, Map<String, Object> parameters) throws IOException {
        try {
            ResultSet result = sessionPool.execute(statement, parameters);
            if (!result.isSucceeded()) {
                throw new IOException(
                        "NebulaGraph rejected the write with code "
                                + result.getErrorCode()
                                + ": "
                                + result.getErrorMessage());
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("NebulaGraph write request failed.", e);
        }
    }

    @Override
    public void close() {
        sessionPool.close();
    }
}
