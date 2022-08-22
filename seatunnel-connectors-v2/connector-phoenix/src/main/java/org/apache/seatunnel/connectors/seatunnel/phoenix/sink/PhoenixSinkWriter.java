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

package org.apache.seatunnel.connectors.seatunnel.phoenix.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.phoenix.client.PhoenixJdbcConnectionProvider;
import org.apache.seatunnel.connectors.seatunnel.phoenix.client.PhoenixOutputFormat;
import org.apache.seatunnel.connectors.seatunnel.phoenix.client.PhoenixStatementExecutor;
import org.apache.seatunnel.connectors.seatunnel.phoenix.config.PhoenixSinkConfig;

import org.apache.commons.lang3.SerializationUtils;

import java.io.IOException;

public class PhoenixSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final PhoenixOutputFormat outputFormat;
    private transient boolean isOpen;

    PhoenixSinkWriter(PhoenixSinkConfig config) {
        PhoenixJdbcConnectionProvider connectionProvider = new PhoenixJdbcConnectionProvider(config);

        this.outputFormat = new PhoenixOutputFormat(
                connectionProvider,
                config,
               new PhoenixStatementExecutor(config, null));
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            isOpen = true;
            outputFormat.open();
        }
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        SeaTunnelRow copy = SerializationUtils.clone(element);
        outputFormat.writeRecord(copy);
    }

    @Override
    public void close() {
        outputFormat.close();
    }
}
