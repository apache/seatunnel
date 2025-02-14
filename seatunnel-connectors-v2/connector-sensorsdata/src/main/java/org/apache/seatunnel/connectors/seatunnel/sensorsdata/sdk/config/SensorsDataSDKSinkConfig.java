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

package org.apache.seatunnel.connectors.seatunnel.sensorsdata.sdk.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.format.sensorsdata.config.SensorsDataConfigBase;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public class SensorsDataSDKSinkConfig extends SensorsDataConfigBase {

    private final String serverUrl;
    private final int bulkSize;
    private final int maxCacheRowSize;
    private final String consumer;

    public SensorsDataSDKSinkConfig(ReadonlyConfig config) {
        super(config);
        // server 相关
        this.serverUrl = config.get(SensorsDataSDKOptions.SERVER_URL);
        this.bulkSize = config.get(SensorsDataSDKOptions.BULK_SIZE);
        this.maxCacheRowSize = config.get(SensorsDataSDKOptions.MAX_CACHE_ROW_SIZE);
        this.consumer = config.get(SensorsDataSDKOptions.CONSUMER);
    }
}
