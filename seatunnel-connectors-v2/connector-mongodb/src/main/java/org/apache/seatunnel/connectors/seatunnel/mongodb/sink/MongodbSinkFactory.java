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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
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
                .required(MongodbConfig.URI, MongodbConfig.DATABASE, MongodbConfig.COLLECTION)
                .optional(
                        MongodbConfig.BUFFER_FLUSH_INTERVAL,
                        MongodbConfig.BUFFER_FLUSH_MAX_ROWS,
                        MongodbConfig.RETRY_MAX,
                        MongodbConfig.RETRY_INTERVAL,
                        MongodbConfig.UPSERT_ENABLE,
                        MongodbConfig.PRIMARY_KEY)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        String connection = readonlyConfig.get(MongodbConfig.URI);
        String database = readonlyConfig.get(MongodbConfig.DATABASE);
        String collection = readonlyConfig.get(MongodbConfig.COLLECTION);
        MongodbWriterOptions.Builder builder =
                MongodbWriterOptions.builder()
                        .withConnectString(connection)
                        .withDatabase(database)
                        .withCollection(collection);
        if (readonlyConfig.getOptional(MongodbConfig.BUFFER_FLUSH_MAX_ROWS).isPresent()) {
            builder.withFlushSize(readonlyConfig.get(MongodbConfig.BUFFER_FLUSH_MAX_ROWS));
        }
        if (readonlyConfig.getOptional(MongodbConfig.BUFFER_FLUSH_INTERVAL).isPresent()) {
            builder.withBatchIntervalMs(readonlyConfig.get(MongodbConfig.BUFFER_FLUSH_INTERVAL));
        }
        if (readonlyConfig.getOptional(MongodbConfig.PRIMARY_KEY).isPresent()) {
            builder.withPrimaryKey(
                    readonlyConfig.get(MongodbConfig.PRIMARY_KEY).toArray(new String[0]));
        }
        if (readonlyConfig.getOptional(MongodbConfig.UPSERT_ENABLE).isPresent()) {
            builder.withUpsertEnable(readonlyConfig.get(MongodbConfig.UPSERT_ENABLE));
        }
        if (readonlyConfig.getOptional(MongodbConfig.RETRY_MAX).isPresent()) {
            builder.withRetryMax(readonlyConfig.get(MongodbConfig.RETRY_MAX));
        }
        if (readonlyConfig.getOptional(MongodbConfig.RETRY_INTERVAL).isPresent()) {
            builder.withRetryInterval(readonlyConfig.get(MongodbConfig.RETRY_INTERVAL));
        }

        if (readonlyConfig.getOptional(MongodbConfig.TRANSACTION).isPresent()) {
            builder.withTransaction(readonlyConfig.get(MongodbConfig.TRANSACTION));
        }
        return () -> new MongodbSink(builder.build(), context.getCatalogTable());
    }
}
