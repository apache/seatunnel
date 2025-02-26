/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class AerospikeSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return "aerospike";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        AerospikeSinkOptions.HOST,
                        AerospikeSinkOptions.PORT,
                        AerospikeSinkOptions.NAMESPACE,
                        AerospikeSinkOptions.SET)
                .optional(
                        AerospikeSinkOptions.USERNAME,
                        AerospikeSinkOptions.PASSWORD,
                        AerospikeSinkOptions.KEY_FIELD,
                        AerospikeSinkOptions.BIN_NAME,
                        AerospikeSinkOptions.DATA_FORMAT,
                        AerospikeSinkOptions.WRITE_TIMEOUT)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new AerospikeSink(context.getOptions(), context.getCatalogTable());
    }
}
