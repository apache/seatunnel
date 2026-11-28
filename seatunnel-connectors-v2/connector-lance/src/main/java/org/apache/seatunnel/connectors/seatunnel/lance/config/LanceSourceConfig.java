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

import com.lancedb.lance.ReadOptions;

import java.util.HashMap;
import java.util.Map;

public class LanceSourceConfig extends LanceCommonConfig {

    private final Map<String, String> storageOptions;

    public LanceSourceConfig(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        this.storageOptions = pluginConfig.get(LanceSourceOptions.READ_STORAGE_OPTIONS);
    }

    public Map<String, String> getStorageOptions() {
        return storageOptions;
    }

    /**
     * Builds the lance-core read options carrying the storage options, so dataset paths on object
     * storage (e.g. s3://) authenticate with the same credentials as the sink.
     */
    public ReadOptions toReadOptions() {
        return new ReadOptions.Builder()
                .setStorageOptions(storageOptions == null ? new HashMap<>() : storageOptions)
                .build();
    }
}
