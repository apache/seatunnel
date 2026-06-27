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

package org.apache.seatunnel.connectors.seatunnel.couchbase.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.couchbase.config.CouchbaseSinkOptions;

import com.google.auto.service.AutoService;

/**
 * Factory class that creates {@link CouchbaseSink} instances from user-provided configuration.
 *
 * <p>Registered via Java SPI so the SeaTunnel runtime discovers it automatically.
 */
@AutoService(Factory.class)
public class CouchbaseSinkFactory implements TableSinkFactory {

    @Override
    public String factoryIdentifier() {
        return CouchbaseSinkOptions.CONNECTOR_IDENTITY;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        CouchbaseSinkOptions.CONNECTION_STRING,
                        CouchbaseSinkOptions.USERNAME,
                        CouchbaseSinkOptions.PASSWORD,
                        CouchbaseSinkOptions.BUCKET,
                        CouchbaseSinkOptions.COLLECTION)
                .optional(
                        CouchbaseSinkOptions.SCOPE,
                        CouchbaseSinkOptions.BUFFER_FLUSH_MAX_ROWS,
                        CouchbaseSinkOptions.BUFFER_FLUSH_INTERVAL,
                        CouchbaseSinkOptions.RETRY_MAX,
                        CouchbaseSinkOptions.RETRY_INTERVAL,
                        CouchbaseSinkOptions.UPSERT_ENABLE,
                        CouchbaseSinkOptions.PRIMARY_KEY)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig config = context.getOptions();

        CouchbaseWriterOptions.Builder builder =
                CouchbaseWriterOptions.builder()
                        .withConnectionString(config.get(CouchbaseSinkOptions.CONNECTION_STRING))
                        .withUsername(config.get(CouchbaseSinkOptions.USERNAME))
                        .withPassword(config.get(CouchbaseSinkOptions.PASSWORD))
                        .withBucket(config.get(CouchbaseSinkOptions.BUCKET))
                        .withScope(config.get(CouchbaseSinkOptions.SCOPE))
                        .withCollection(config.get(CouchbaseSinkOptions.COLLECTION));

        config.getOptional(CouchbaseSinkOptions.BUFFER_FLUSH_MAX_ROWS)
                .ifPresent(builder::withFlushSize);
        config.getOptional(CouchbaseSinkOptions.BUFFER_FLUSH_INTERVAL)
                .ifPresent(builder::withBatchIntervalMs);
        config.getOptional(CouchbaseSinkOptions.RETRY_MAX).ifPresent(builder::withRetryMax);
        config.getOptional(CouchbaseSinkOptions.RETRY_INTERVAL)
                .ifPresent(builder::withRetryInterval);
        config.getOptional(CouchbaseSinkOptions.UPSERT_ENABLE).ifPresent(builder::withUpsertEnable);
        config.getOptional(CouchbaseSinkOptions.PRIMARY_KEY)
                .ifPresent(keys -> builder.withPrimaryKey(keys.toArray(new String[0])));

        CatalogTable catalogTable = context.getCatalogTable();
        // Stamp the catalog table with the sink identifier so metrics are attributed correctly.
        String bucket = config.get(CouchbaseSinkOptions.BUCKET);
        String collection = config.get(CouchbaseSinkOptions.COLLECTION);
        TableIdentifier tableIdentifier =
                TableIdentifier.of(CouchbaseSinkOptions.CONNECTOR_IDENTITY, bucket, collection);
        CatalogTable sinkCatalogTable = CatalogTable.of(tableIdentifier, catalogTable);

        CouchbaseWriterOptions options = builder.build();
        return () -> new CouchbaseSink(options, sinkCatalogTable);
    }
}
