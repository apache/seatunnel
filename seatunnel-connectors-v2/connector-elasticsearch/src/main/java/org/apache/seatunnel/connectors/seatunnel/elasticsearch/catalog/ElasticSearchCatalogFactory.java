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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.factory.CatalogFactory;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.AuthTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchValidators.ApiKeyEncodedFormatValidator;

import com.google.auto.service.AutoService;

import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.API_KEY;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.API_KEY_ENCODED;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.API_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.AUTH_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.HOSTS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_KEY_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_KEY_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_TRUST_STORE_PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_TRUST_STORE_PATH;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_VERIFY_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.TLS_VERIFY_HOSTNAME;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchBaseOptions.USERNAME;

@AutoService(Factory.class)
public class ElasticSearchCatalogFactory implements CatalogFactory {

    @Override
    public Catalog createCatalog(String catalogName, ReadonlyConfig options) {
        return new ElasticSearchCatalog(catalogName, "", options);
    }

    @Override
    public String factoryIdentifier() {
        return "Elasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS)
                .optional(
                        USERNAME,
                        PASSWORD,
                        TLS_VERIFY_CERTIFICATE,
                        TLS_VERIFY_HOSTNAME,
                        TLS_KEY_STORE_PATH,
                        TLS_KEY_STORE_PASSWORD,
                        TLS_TRUST_STORE_PATH,
                        TLS_TRUST_STORE_PASSWORD)
                .optional(AUTH_TYPE)
                .conditionalRule(
                        AUTH_TYPE,
                        AuthTypeEnum.BASIC,
                        OptionRule.builder().bundled(USERNAME, PASSWORD).build())
                .conditional(AUTH_TYPE, AuthTypeEnum.BASIC, Conditions.notBlank(USERNAME))
                .conditional(AUTH_TYPE, AuthTypeEnum.BASIC, Conditions.notBlank(PASSWORD))
                .conditional(AUTH_TYPE, AuthTypeEnum.API_KEY, API_KEY_ID, API_KEY)
                .conditional(AUTH_TYPE, AuthTypeEnum.API_KEY, Conditions.notBlank(API_KEY_ID))
                .conditional(AUTH_TYPE, AuthTypeEnum.API_KEY, Conditions.notBlank(API_KEY))
                .conditional(AUTH_TYPE, AuthTypeEnum.API_KEY_ENCODED, API_KEY_ENCODED)
                .conditional(
                        AUTH_TYPE,
                        AuthTypeEnum.API_KEY_ENCODED,
                        Conditions.notBlank(API_KEY_ENCODED))
                .conditional(
                        AUTH_TYPE,
                        AuthTypeEnum.API_KEY_ENCODED,
                        Conditions.extension(API_KEY_ENCODED, new ApiKeyEncodedFormatValidator()))
                .build();
    }
}
