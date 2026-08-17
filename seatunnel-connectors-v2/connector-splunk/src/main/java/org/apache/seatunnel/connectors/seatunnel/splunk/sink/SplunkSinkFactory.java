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

package org.apache.seatunnel.connectors.seatunnel.splunk.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.splunk.config.SplunkSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SplunkSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return SplunkSink.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SplunkSinkOptions.URL, SplunkSinkOptions.TOKEN)
                .optional(
                        SplunkSinkOptions.INDEX,
                        SplunkSinkOptions.SOURCE,
                        SplunkSinkOptions.SOURCE_TYPE,
                        SplunkSinkOptions.HOST,
                        SplunkSinkOptions.HOST_FIELD,
                        SplunkSinkOptions.TIME_FIELD,
                        SplunkSinkOptions.MAX_BATCH_SIZE,
                        SplunkSinkOptions.MAX_RETRY_COUNT,
                        SplunkSinkOptions.RETRY_BACKOFF_MS,
                        SplunkSinkOptions.CONNECT_TIMEOUT_MS,
                        SplunkSinkOptions.SOCKET_TIMEOUT_MS,
                        SplunkSinkOptions.TLS_VERIFY_CERTIFICATE,
                        SplunkSinkOptions.TLS_VERIFY_HOSTNAME,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        SplunkSinkConfig config = new SplunkSinkConfig(context.getOptions());
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new SplunkSink(config, catalogTable);
    }
}
