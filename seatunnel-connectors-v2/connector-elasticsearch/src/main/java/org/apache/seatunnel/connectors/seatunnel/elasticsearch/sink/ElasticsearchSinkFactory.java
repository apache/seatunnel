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
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.SinkConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.SupportSinkDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.AuthTypeEnum;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchValidators.ApiKeyEncodedFormatValidator;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

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
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.INDEX;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.INDEX_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.KEY_DELIMITER;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.MAX_BATCH_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.MAX_RETRY_COUNT;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.PRIMARY_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTORIZATION_FIELDS;
import static org.apache.seatunnel.connectors.seatunnel.elasticsearch.config.ElasticsearchSinkOptions.VECTOR_DIMENSIONS;

@AutoService(Factory.class)
@Slf4j
public class ElasticsearchSinkFactory implements TableSinkFactory, SupportSinkDryRunValidation {
    @Override
    public String factoryIdentifier() {
        return "Elasticsearch";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(HOSTS)
                .required(
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
                        SinkConnectorCommonOptions.MULTI_TABLE_SINK_REPLICA,
                        VECTORIZATION_FIELDS,
                        VECTOR_DIMENSIONS)
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

    @Override
    public void validateConnectionForDryRun(TableSinkFactoryContext context) throws Exception {
        validateElasticsearchConnection(context.getOptions());
    }

    /**
     * Validates Elasticsearch/OpenSearch connectivity and authentication. This method is only
     * invoked by {@link #validateConnectionForDryRun(TableSinkFactoryContext)} during a dry-run, so
     * that connection issues are surfaced up-front without introducing an extra connection/failure
     * point on the production submission or resume path.
     */
    private void validateElasticsearchConnection(ReadonlyConfig config) {
        try (EsRestClient client = EsRestClient.createInstance(config)) {
            // 1. Validate connectivity and authentication by fetching cluster info (GET /)
            ElasticsearchClusterInfo clusterInfo = client.getClusterInfo();
            if (clusterInfo == null) {
                throw new SeaTunnelException(
                        "Failed to retrieve cluster info from Elasticsearch/OpenSearch: "
                                + "getClusterInfo() returned null");
            }
            log.info(
                    "Successfully connected to {} cluster, version={}",
                    clusterInfo.isOpensearch() ? "OpenSearch" : "Elasticsearch",
                    clusterInfo.getClusterVersion());

            // 2. Validate target index accessibility (HEAD /{index})
            // Skip validation for dynamic index names containing placeholders
            String index = config.get(INDEX);
            if (index != null && !index.contains(ElasticsearchSinkOptions.INDEX_VARIABLE_PREFIX)) {
                // checkIndexExist returns true/false for index existence;
                // if credentials lack read permission, it throws an exception.
                boolean exists = client.checkIndexExist(index);
                log.info("Target index '{}' exists={}", index, exists);
            }
        } catch (Exception e) {
            throw new SeaTunnelException(
                    "Failed to validate Elasticsearch/OpenSearch connection: " + e.getMessage(), e);
        }
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
