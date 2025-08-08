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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.AuthTypeEnum;

import com.google.auto.service.AutoService;

import java.io.Serializable;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.API_KEY;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.API_KEY_ENCODED;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.API_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.AUTH_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.HOSTS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.INDEX;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_KEY_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_KEY_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_TRUST_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_TRUST_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_VERIFY_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_VERIFY_HOSTNAME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.USERNAME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.INDEX_LIST;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.PIT_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.PIT_KEEP_ALIVE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.QUERY;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.SCROLL_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.SCROLL_TIME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.SEARCH_API_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSourceOptions.SEARCH_TYPE;

@AutoService(Factory.class)
public class ElasticsearchSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "Elasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS)
                .optional(
                        INDEX,
                        INDEX_LIST,
                        USERNAME,
                        PASSWORD,
                        SCROLL_TIME,
                        SCROLL_SIZE,
                        QUERY,
                        PIT_KEEP_ALIVE,
                        PIT_BATCH_SIZE,
                        SEARCH_API_TYPE,
                        SEARCH_TYPE,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD)
                .optional(AUTH_TYPE)
                .conditional(AUTH_TYPE, AuthTypeEnum.API_KEY, API_KEY_ID, API_KEY)
                .conditional(AUTH_TYPE, AuthTypeEnum.API_KEY_ENCODED, API_KEY_ENCODED)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>) new ElasticsearchSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return ElasticsearchSource.class;
    }
}
