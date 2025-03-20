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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.HOSTS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_KEY_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_KEY_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_TRUST_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_TRUST_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_VERIFY_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_VERIFY_HOSTNAME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.INDEX;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.KEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.MAX_RETRY_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.PRIMARY_KEYS;

@AutoService(Factory.class)
public class ElasticsearchSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return "Elasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        HOSTS,
                        INDEX,
                        ElasticsearchSinkOptions.SCHEMA_SAVE_MODE,
                        ElasticsearchSinkOptions.DATA_SAVE_MODE)
                .optional(
                        INDEX_TYPE,
                        PRIMARY_KEYS,
                        KEY_DELIMITER,
                        USERNAME,
                        PASSWORD,
                        MAX_RETRY_COUNT,
                        MAX_BATCH_SIZE,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD,
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        ReadonlyConfig readonlyConfig = context.getOptions();
        String original = readonlyConfig.get(INDEX);
        CatalogTable newTable =
                CatalogTable.of(
                        TableIdentifier.of(
                                context.getCatalogTable().getCatalogName(),
                                context.getCatalogTable().getTablePath().getDatabaseName(),
                                original),
                        context.getCatalogTable());
        return () -> new ElasticsearchSink(readonlyConfig, newTable);
    }
}
