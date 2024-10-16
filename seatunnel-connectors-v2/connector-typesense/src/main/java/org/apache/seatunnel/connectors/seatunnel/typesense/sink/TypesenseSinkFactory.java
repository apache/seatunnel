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

package org.apache.seatunnel.connectors.seatunnel.typesense.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig.KEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.SinkConfig.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig.APIKEY;
import static org.apache.seatunnel.connectors.seatunnel.typesense.config.TypesenseConnectionConfig.HOSTS;

@AutoService(Factory.class)
public class TypesenseSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "Typesense";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        HOSTS,
                        COLLECTION,
                        APIKEY,
                        SinkConfig.SCHEMA_SAVE_MODE,
                        SinkConfig.DATA_SAVE_MODE)
                .optional(PRIMARY_KEYS, KEY_DELIMITER)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        String original = readonlyConfig.get(COLLECTION);
        CatalogTable newTable =
                CatalogTable.of(
                        TableIdentifier.of(
                                context.getCatalogTable().getCatalogName(),
                                context.getCatalogTable().getTablePath().getDatabaseName(),
                                original),
                        context.getCatalogTable());
        return () -> new TypesenseSink(readonlyConfig, newTable);
    }
}
