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
import org.apache.seatunnel.connectors.seatunnel.hudi.sink.HudiClientManager;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;

public class HudiWriteClientProviderProxy implements WriteClientProvider {

    private final HudiClientManager clientManager;

    private final Integer index;

    private final String tableName;

    private final SeaTunnelRowType seaTunnelRowType;

    public HudiWriteClientProviderProxy(
            HudiClientManager clientManager,
            SeaTunnelRowType seaTunnelRowType,
            int index,
            String tableName) {
        this.clientManager = clientManager;
        this.seaTunnelRowType = seaTunnelRowType;
        this.index = index;
        this.tableName = tableName;
    }

    @Override
    public HoodieJavaWriteClient<HoodieAvroPayload> getOrCreateClient() {
        return clientManager.getClient(this.index, tableName, seaTunnelRowType);
    }

    @Override
    public void close() {
        if (clientManager.containsClient(tableName, index)) {
            clientManager.remove(tableName, index).close();
        }
    }
}
