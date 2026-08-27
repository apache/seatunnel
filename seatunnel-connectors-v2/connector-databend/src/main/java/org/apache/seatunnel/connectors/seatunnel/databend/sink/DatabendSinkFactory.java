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

package org.apache.seatunnel.connectors.seatunnel.databend.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendOptions;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DatabendSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Databend";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(DatabendOptions.URL, DatabendOptions.USERNAME, DatabendOptions.PASSWORD)
                .optional(
                        DatabendOptions.DATABASE,
                        DatabendOptions.TABLE,
                        DatabendOptions.JDBC_CONFIG,
                        DatabendOptions.BATCH_SIZE,
                        DatabendOptions.AUTO_COMMIT,
                        DatabendOptions.MAX_RETRIES,
                        DatabendSinkOptions.SCHEMA_SAVE_MODE,
                        DatabendSinkOptions.DATA_SAVE_MODE,
                        DatabendSinkOptions.CUSTOM_SQL,
                        DatabendSinkOptions.EXECUTE_TIMEOUT_SEC)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> {
            CatalogTable catalogTable = context.getCatalogTable();
            return new DatabendSink(catalogTable, context.getOptions());
        };
    }
}
