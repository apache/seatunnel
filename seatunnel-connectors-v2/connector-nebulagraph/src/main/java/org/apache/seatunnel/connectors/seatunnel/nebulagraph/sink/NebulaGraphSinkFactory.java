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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.config.NebulaGraphSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class NebulaGraphSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return NebulaGraphSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        NebulaGraphSinkConfig config = NebulaGraphSinkConfig.of(context.getOptions());
        return () -> new NebulaGraphSink(config, context.getCatalogTable());
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        NebulaGraphSinkOptions.HOSTS,
                        NebulaGraphSinkOptions.USERNAME,
                        NebulaGraphSinkOptions.PASSWORD,
                        NebulaGraphSinkOptions.SPACE,
                        NebulaGraphSinkOptions.TAG,
                        NebulaGraphSinkOptions.VID_FIELD)
                .optional(
                        NebulaGraphSinkOptions.WRITE_FIELDS,
                        NebulaGraphSinkOptions.WRITE_MODE,
                        NebulaGraphSinkOptions.BATCH_SIZE,
                        NebulaGraphSinkOptions.TIMEOUT_MILLIS,
                        NebulaGraphSinkOptions.MAX_RETRIES,
                        NebulaGraphSinkOptions.RETRY_INTERVAL_MILLIS)
                .build();
    }
}
