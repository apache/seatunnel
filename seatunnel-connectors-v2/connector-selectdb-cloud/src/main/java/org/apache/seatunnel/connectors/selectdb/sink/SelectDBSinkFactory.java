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

package org.apache.seatunnel.connectors.selectdb.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.selectdb.config.SelectDBSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class SelectDBSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return SelectDBSinkOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        SelectDBSinkOptions.JDBC_URL,
                        SelectDBSinkOptions.LOAD_URL,
                        SelectDBSinkOptions.CLUSTER_NAME,
                        SelectDBSinkOptions.USERNAME,
                        SelectDBSinkOptions.TABLE_IDENTIFIER)
                .optional(
                        SelectDBSinkOptions.PASSWORD,
                        SelectDBSinkOptions.SINK_ENABLE_2PC,
                        SelectDBSinkOptions.SINK_MAX_RETRIES,
                        SelectDBSinkOptions.SINK_BUFFER_SIZE,
                        SelectDBSinkOptions.SINK_BUFFER_COUNT,
                        SelectDBSinkOptions.SINK_LABEL_PREFIX,
                        SelectDBSinkOptions.SINK_ENABLE_DELETE,
                        SelectDBSinkOptions.SINK_FLUSH_QUEUE_SIZE,
                        SelectDBSinkOptions.SELECTDB_SINK_CONFIG_PREFIX)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new SelectDBSink(context.getOptions(), context.getCatalogTable());
    }
}
