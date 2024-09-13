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

package org.apache.seatunnel.connectors.seatunnel.hudi.sink.client;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hudi.config.HudiSinkConfig;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil.createHoodieJavaWriteClient;

@Slf4j
public class HudiWriteClientProvider implements WriteClientProvider, Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(HudiWriteClientProvider.class);

    private transient HoodieJavaWriteClient<HoodieAvroPayload> client;

    private final HudiSinkConfig hudiSinkConfig;

    private final String tableName;

    private final SeaTunnelRowType seaTunnelRowType;

    public HudiWriteClientProvider(
            HudiSinkConfig hudiSinkConfig, String tableName, SeaTunnelRowType seaTunnelRowType) {
        this.hudiSinkConfig = hudiSinkConfig;
        this.tableName = tableName;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public HoodieJavaWriteClient<HoodieAvroPayload> getOrCreateClient() {
        if (client == null) {
            client = createHoodieJavaWriteClient(hudiSinkConfig, seaTunnelRowType, tableName);
        }
        return client;
    }

    @Override
    public void close() {
        try {
            if (client != null) {
                client.close();
            }
        } catch (Exception e) {
            LOG.error("hudi client close failed.", e);
        }
    }
}
