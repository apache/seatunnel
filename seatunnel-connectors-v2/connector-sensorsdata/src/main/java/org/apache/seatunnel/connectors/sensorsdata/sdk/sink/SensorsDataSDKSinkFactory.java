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

package org.apache.seatunnel.connectors.sensorsdata.sdk.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataBaseOptionRules;
import org.apache.seatunnel.connectors.sensorsdata.format.config.SensorsDataOptions;
import org.apache.seatunnel.connectors.sensorsdata.sdk.config.SensorsDataSDKSinkConfig;
import org.apache.seatunnel.connectors.sensorsdata.sdk.config.SensorsDataSDKSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SensorsDataSDKSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "SensorsData";
    }

    @Override
    public OptionRule optionRule() {
        return SensorsDataBaseOptionRules.getBaseOptionRuleBuilder()
                .optional(
                        SensorsDataSDKSinkOptions.BULK_SIZE,
                        SensorsDataSDKSinkOptions.MAX_CACHE_ROW_SIZE,
                        SensorsDataOptions.SKIP_ERROR_RECORD,
                        SensorsDataSDKSinkOptions.INSTANT_EVENT_LIST)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        SensorsDataSDKSinkConfig sinkConfig = new SensorsDataSDKSinkConfig(config);
        return () -> new SensorsDataSDKSink(sinkConfig, catalogTable);
    }
}
