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

package org.apache.seatunnel.engine.common.config.server;

import lombok.Data;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

@Data
public class ConnectorJarStorageConfig {
    private Boolean enable = ServerConfigOptions.ENABLE_CONNECTOR_JAR_STORAGE.defaultValue();

    private ConnectorJarStorageMode storageMode =
            ServerConfigOptions.CONNECTOR_JAR_STORAGE_MODE.defaultValue();

    private String storagePath = ServerConfigOptions.CONNECTOR_JAR_STORAGE_PATH.defaultValue();

    private Integer cleanupTaskInterval =
            ServerConfigOptions.CONNECTOR_JAR_CLEANUP_TASK_INTERVAL.defaultValue();

    private Integer connectorJarExpiryTime =
            ServerConfigOptions.CONNECTOR_JAR_EXPIRY_TIME.defaultValue();

    private ConnectorJarHAStorageConfig connectorJarHAStorageConfig =
            ServerConfigOptions.CONNECTOR_JAR_HA_STORAGE_CONFIG.defaultValue();

    public ConnectorJarStorageConfig setStorageMode(ConnectorJarStorageMode storageMode) {
        checkNotNull(storageMode);
        this.storageMode = storageMode;
        return this;
    }
}
