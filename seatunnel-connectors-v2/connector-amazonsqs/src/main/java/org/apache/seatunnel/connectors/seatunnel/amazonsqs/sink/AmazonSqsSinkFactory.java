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

package org.apache.seatunnel.connectors.seatunnel.amazonsqs.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions.FIELD_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions.FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions.SECRET_ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amazonsqs.config.AmazonSqsSinkOptions.URL;

@AutoService(Factory.class)
public class AmazonSqsSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "AmazonSqs";
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new AmazonSqsSink(config, catalogTable);
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, REGION)
                .optional(ACCESS_KEY_ID, SECRET_ACCESS_KEY, FORMAT, FIELD_DELIMITER)
                .build();
    }
}
