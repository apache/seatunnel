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
package org.apache.seatunnel.connectors.seatunnel.mem0.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.mem0.config.Mem0SinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class Mem0SinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Mem0";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(Mem0SinkOptions.API_KEY, Mem0SinkOptions.MESSAGES_FIELD)
                .optional(
                        Mem0SinkOptions.API_BASE_URL,
                        Mem0SinkOptions.AGENT_ID_FIELD,
                        Mem0SinkOptions.APP_ID_FIELD,
                        Mem0SinkOptions.RUN_ID_FIELD,
                        Mem0SinkOptions.METADATA_FIELD,
                        Mem0SinkOptions.RETRY,
                        Mem0SinkOptions.RETRY_BACKOFF_MULTIPLIER_MS,
                        Mem0SinkOptions.RETRY_BACKOFF_MAX_MS,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new Mem0Sink(context.getOptions(), context.getCatalogTable());
    }
}
