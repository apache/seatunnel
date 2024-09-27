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

package org.apache.seatunnel.connectors.seatunnel.email.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.email.config.EmailConfig;
import org.apache.seatunnel.connectors.seatunnel.email.config.EmailSinkConfig;

public class EmailSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private SeaTunnelRowType seaTunnelRowType;
    private ReadonlyConfig readonlyConfig;
    private CatalogTable catalogTable;
    private EmailSinkConfig pluginConfig;

    public EmailSink(ReadonlyConfig config, CatalogTable table) {
        this.readonlyConfig = config;
        this.catalogTable = table;
        this.pluginConfig = new EmailSinkConfig(config);
        this.seaTunnelRowType = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public EmailSinkWriter createWriter(SinkWriter.Context context) {
        return new EmailSinkWriter(seaTunnelRowType, pluginConfig);
    }

    @Override
    public String getPluginName() {
        return EmailConfig.CONNECTOR_IDENTITY;
    }
}
