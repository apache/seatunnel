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

package org.apache.seatunnel.connectors.seatunnel.lance.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import com.lancedb.lance.WriteParams;

import java.util.Map;

public class LanceSinkConfig extends LanceCommonConfig {

    private final Integer maxRowsPerFile;

    private final Integer maxRowsPerGroup;

    private final Long maxBytesPerFile;

    private final WriteParams.WriteMode mode;

    private Boolean enableStableRowIds;

    private final Map<String, String> storageOptions;

    private final String namespaceId;

    public LanceSinkConfig(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        this.namespaceId = pluginConfig.get(LanceCommonOptions.KEY_NAMESPACE_ID);
        this.maxBytesPerFile = pluginConfig.get(LanceSinkOptions.WRITE_MAX_BYTES_PER_FILE);
        this.maxRowsPerGroup = pluginConfig.get(LanceSinkOptions.WRITE_MAX_ROWS_PER_GROUP);
        this.maxRowsPerFile = pluginConfig.get(LanceSinkOptions.WRITE_MAX_ROWS_PER_FILE);
        this.mode = WriteParams.WriteMode.valueOf(pluginConfig.get(LanceSinkOptions.WRITE_MODE));
        this.storageOptions = pluginConfig.get(LanceSinkOptions.WRITE_STORAGE_OPTIONS);
        this.enableStableRowIds = pluginConfig.get(LanceSinkOptions.WRITE_ENABLE_STABLE_ROW_IDS);
    }

    public Integer getMaxRowsPerFile() {
        return maxRowsPerFile;
    }

    public Integer getMaxRowsPerGroup() {
        return maxRowsPerGroup;
    }

    public Long getMaxBytesPerFile() {
        return maxBytesPerFile;
    }

    public WriteParams.WriteMode getMode() {
        return mode;
    }

    public Boolean getEnableStableRowIds() {
        return enableStableRowIds;
    }

    public Map<String, String> getStorageOptions() {
        return storageOptions;
    }

    public String getNamespaceId() {
        return namespaceId;
    }
}
