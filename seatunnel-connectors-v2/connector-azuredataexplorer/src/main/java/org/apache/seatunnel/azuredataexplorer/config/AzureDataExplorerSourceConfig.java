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

package org.apache.seatunnel.azuredataexplorer.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Builder;
import lombok.Getter;

import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.CLUSTER_URI;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.DATABASE;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.QUERY;
import static org.apache.seatunnel.azuredataexplorer.config.AzureDataExplorerSourceOptions.TENANT_ID;

/** Immutable config value object used by the source. */
@Getter
@Builder
public class AzureDataExplorerSourceConfig {

    private final String clusterUri;
    private final String database;
    private final String clientId;
    private final String clientSecret;
    private final String tenantId;
    private final String query;

    public static AzureDataExplorerSourceConfig fromSourceConfig(ReadonlyConfig cfg) {
        return AzureDataExplorerSourceConfig.builder()
                .clusterUri(cfg.get(CLUSTER_URI))
                .database(cfg.get(DATABASE))
                .clientId(cfg.get(CLIENT_ID))
                .clientSecret(cfg.get(CLIENT_SECRET))
                .tenantId(cfg.get(TENANT_ID))
                .query(cfg.get(QUERY))
                .build();
    }
}
