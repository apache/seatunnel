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

package org.apache.seatunnel.connectors.seatunnel.tablestore.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TableStoreSinkOptions;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;
import static org.apache.seatunnel.api.configuration.util.Conditions.notEmpty;

@AutoService(Factory.class)
public class TableStoreSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return TableStoreSinkOptions.identifier;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        TableStoreSinkOptions.END_POINT, notBlank(TableStoreSinkOptions.END_POINT))
                .required(TableStoreSinkOptions.TABLE, notBlank(TableStoreSinkOptions.TABLE))
                .required(
                        TableStoreSinkOptions.INSTANCE_NAME,
                        notBlank(TableStoreSinkOptions.INSTANCE_NAME))
                .required(
                        TableStoreSinkOptions.ACCESS_KEY_ID,
                        notBlank(TableStoreSinkOptions.ACCESS_KEY_ID))
                .required(
                        TableStoreSinkOptions.ACCESS_KEY_SECRET,
                        notBlank(TableStoreSinkOptions.ACCESS_KEY_SECRET))
                .required(
                        TableStoreSinkOptions.PRIMARY_KEYS,
                        notEmpty(TableStoreSinkOptions.PRIMARY_KEYS))
                .required(ConnectorCommonOptions.SCHEMA)
                .optional(TableStoreSinkOptions.BATCH_SIZE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new TableStoreSink(context.getOptions(), context.getCatalogTable());
    }
}
