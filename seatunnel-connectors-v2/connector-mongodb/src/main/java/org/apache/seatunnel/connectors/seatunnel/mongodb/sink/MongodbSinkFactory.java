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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(Factory.class)
public class MongodbSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        MongodbConfig.URI,
                        MongodbConfig.DATABASE,
                        MongodbConfig.COLLECTION,
                        ConnectorCommonOptions.SCHEMA)
                .optional(
                        MongodbConfig.BUFFER_FLUSH_INTERVAL,
                        MongodbConfig.BUFFER_FLUSH_MAX_ROWS,
                        MongodbConfig.RETRY_MAX,
                        MongodbConfig.RETRY_INTERVAL,
                        MongodbConfig.UPSERT_ENABLE,
                        MongodbConfig.PRIMARY_KEY)
                .build();
    }
}
