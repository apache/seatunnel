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
import org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiAuthenticationContext;

import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.common.model.HoodieAvroPayload;

import java.io.Serializable;
import java.util.function.Supplier;

import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil.createHoodieJavaWriteClient;
import static org.apache.seatunnel.connectors.seatunnel.hudi.util.HudiUtil.getConfiguration;

public class HudiWriteClientProvider implements WriteClientProvider, Serializable {

    private transient HoodieJavaWriteClient<HoodieAvroPayload> client;

    private transient HudiAuthenticationContext authentication;

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
    public synchronized HoodieJavaWriteClient<HoodieAvroPayload> getOrCreateClient() {
        if (client == null) {
            client =
                    execute(
                            () ->
                                    createHoodieJavaWriteClient(
                                            hudiSinkConfig,
                                            seaTunnelRowType,
                                            tableName,
                                            authentication.getConfiguration()));
        }
        return client;
    }

    @Override
    public <T> T execute(Supplier<T> action) {
        return getAuthentication().execute(action::get);
    }

    private synchronized HudiAuthenticationContext getAuthentication() {
        if (authentication == null) {
            authentication =
                    new HudiAuthenticationContext(
                            getConfiguration(hudiSinkConfig.getConfFilesPath()),
                            hudiSinkConfig.getKerberosPrincipal(),
                            hudiSinkConfig.getKerberosKeytabPath(),
                            hudiSinkConfig.getKrb5Path());
        }
        return authentication;
    }

    @Override
    public void close() {
        if (authentication != null) {
            authentication.close(
                    () -> {
                        if (client != null) {
                            client.close();
                        }
                        return null;
                    });
        }
    }
}
