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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageMode;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.server.SeaTunnelServer;

public class StorageStrategyFactory {

    public StorageStrategyFactory() {}

    public static ConnectorJarStorageStrategy of(
            ConnectorJarStorageMode connectorJarStorageMode,
            ConnectorJarStorageConfig connectorJarStorageConfig,
            SeaTunnelServer seaTunnelServer) {
        switch (connectorJarStorageMode) {
            case SHARED:
                return new SharedConnectorJarStorageStrategy(
                        connectorJarStorageConfig, seaTunnelServer);
            case ISOLATED:
                return new IsolatedConnectorJarStorageStrategy(
                        connectorJarStorageConfig, seaTunnelServer);
            default:
                throw new IllegalArgumentException(
                        ServerConfigOptions.CONNECTOR_JAR_STORAGE_MODE
                                + " must in [SHARED, ISOLATED]");
        }
    }
}
