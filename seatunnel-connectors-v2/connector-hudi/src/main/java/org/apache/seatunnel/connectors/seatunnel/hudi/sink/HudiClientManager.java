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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil.createHoodieJavaWriteClient;

@Slf4j
public class HudiClientManager {

    private final HudiSinkConfig hudiSinkConfig;

    private final Map<String, Map<Integer, HoodieJavaWriteClient<HoodieAvroPayload>>>
            hoodieJavaWriteClientMap;

    public HudiClientManager(HudiSinkConfig hudiSinkConfig) {
        this.hudiSinkConfig = hudiSinkConfig;
        this.hoodieJavaWriteClientMap = new ConcurrentHashMap<>();
    }

    public HoodieJavaWriteClient<HoodieAvroPayload> getClient(
            int index, String tableName, SeaTunnelRowType seaTunnelRowType) {
        return hoodieJavaWriteClientMap
                .computeIfAbsent(tableName, i -> new ConcurrentHashMap<>())
                .computeIfAbsent(
                        index,
                        i ->
                                createHoodieJavaWriteClient(
                                        hudiSinkConfig, seaTunnelRowType, tableName));
    }

    public boolean containsClient(String tableName, int index) {
        return hoodieJavaWriteClientMap.containsKey(tableName)
                && hoodieJavaWriteClientMap.get(tableName).containsKey(index);
    }

    public HoodieJavaWriteClient<HoodieAvroPayload> remove(String tableName, int index) {
        return hoodieJavaWriteClientMap.get(tableName).get(index);
    }
}
