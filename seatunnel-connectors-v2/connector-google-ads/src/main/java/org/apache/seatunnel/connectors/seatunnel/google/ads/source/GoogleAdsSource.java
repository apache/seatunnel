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

package org.apache.seatunnel.connectors.seatunnel.google.ads.source;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.google.ads.client.GoogleAdsClient;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsTableConfig;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class GoogleAdsSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private static final String PLUGIN_NAME = "GoogleAds";
    private static final String DEFAULT_DATABASE = "google_ads";
    private static final Pattern FIELD_PATH_PATTERN =
            Pattern.compile("^[a-z0-9_]+(\\.[a-z0-9_]+)+$");
    private static final Pattern GAQL_PATTERN =
            Pattern.compile(
                    "^\\s*SELECT\\s+(.+?)\\s+FROM\\s+([a-z0-9_]+)\\b.*$",
                    Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    private final GoogleAdsParameters params;
    private final List<GoogleAdsTableConfig> tableConfigs;

    public GoogleAdsSource(GoogleAdsParameters params, ReadonlyConfig config) {
        this.params = params;
        this.tableConfigs = buildTableConfigs(params, config);
    }

    /**
     * Resolves resource/fields/query/tables_configs into one or more GoogleAdsTableConfig
     * instances, deriving each CatalogTable from the GoogleAdsFieldService. Runs once during
     * factory createSource with a one-shot client scoped to this call.
     */
    private List<GoogleAdsTableConfig> buildTableConfigs(
            GoogleAdsParameters params, ReadonlyConfig config) {
        try (GoogleAdsClient client = new GoogleAdsClient(params)) {
            client.authenticate();

            if (config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
                List<Map<String, Object>> tableConfigMaps =
                        config.get(ConnectorCommonOptions.TABLE_CONFIGS);
                List<GoogleAdsTableConfig> configs = new ArrayList<>();
                for (Map<String, Object> map : tableConfigMaps) {
                    ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(map);
                    GoogleAdsTableConfig built = buildOneTableConfig(tableConfig, client);
                    String tableId = built.getTableId();
                    boolean duplicate =
                            configs.stream().anyMatch(c -> c.getTableId().equals(tableId));
                    if (duplicate) {
                        throw new GoogleAdsConnectorException(
                                GoogleAdsConnectorErrorCode.DUPLICATE_RESOURCE,
                                "Duplicate table in tables_configs: " + tableId);
                    }
                    configs.add(built);
                }
                return configs;
            } else {
                return Collections.singletonList(
                        buildSingleTableConfig(config, DEFAULT_DATABASE, null, client));
            }
        } catch (GoogleAdsConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new GoogleAdsConnectorException(
                    GoogleAdsConnectorErrorCode.DESCRIBE_FIELDS_FAILED,
                    "Failed to build Google Ads table configs",
                    e);
        }
    }

    private GoogleAdsTableConfig buildOneTableConfig(
            ReadonlyConfig tableConfig, GoogleAdsClient client) {
        String tablePath =
                tableConfig
                        .getOptional(GoogleAdsSourceOptions.TABLE_PATH)
                        .orElseThrow(
                                () ->
                                        new GoogleAdsConnectorException(
                                                GoogleAdsConnectorErrorCode.INVALID_TABLE_PATH,
                                                "table_path is required in tables_configs"));
        String[] parts = tablePath.split("\\.", 2);
        if (parts.length != 2 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])) {
            throw new GoogleAdsConnectorException(
                    GoogleAdsConnectorErrorCode.INVALID_TABLE_PATH,
                    "table_path must be 'database.resource', got: " + tablePath);
        }
        return buildSingleTableConfig(tableConfig, parts[0], parts[1], client);
    }

    /**
     * Builds one table from either fields mode (resource + fields [+ filter]) or query mode (full
     * GAQL). The ordered field list parsed here is the single source of truth shared by the schema
     * (describeFields), the query, and row value extraction. expectedResource is non-null only in
     * tables_configs mode, where the table_path resource must agree with the query.
     */
    private GoogleAdsTableConfig buildSingleTableConfig(
            ReadonlyConfig config,
            String database,
            String expectedResource,
            GoogleAdsClient client) {
        String query = config.getOptional(GoogleAdsSourceOptions.QUERY).orElse(null);
        List<String> fields = config.getOptional(GoogleAdsSourceOptions.FIELDS).orElse(null);
        String filter = config.getOptional(GoogleAdsSourceOptions.FILTER).orElse(null);

        String gaql;
        String resource;
        List<String> fieldPaths;
        if (query != null) {
            if (fields != null || filter != null) {
                throw new GoogleAdsConnectorException(
                        GoogleAdsConnectorErrorCode.INVALID_QUERY,
                        "query is mutually exclusive with fields and filter");
            }
            Matcher matcher = GAQL_PATTERN.matcher(query);
            if (!matcher.matches()) {
                throw new GoogleAdsConnectorException(
                        GoogleAdsConnectorErrorCode.INVALID_QUERY,
                        "Cannot parse GAQL query; expected 'SELECT <fields> FROM <resource> ...', "
                                + "got: "
                                + query);
            }
            fieldPaths = new ArrayList<>();
            for (String token : matcher.group(1).split(",")) {
                fieldPaths.add(validateFieldPath(token.trim()));
            }
            resource = matcher.group(2);
            if (expectedResource != null && !expectedResource.equals(resource)) {
                throw new GoogleAdsConnectorException(
                        GoogleAdsConnectorErrorCode.INVALID_QUERY,
                        "table_path resource '"
                                + expectedResource
                                + "' does not match query FROM resource '"
                                + resource
                                + "'");
            }
            gaql = query;
        } else {
            resource =
                    expectedResource != null
                            ? expectedResource
                            : config.getOptional(GoogleAdsSourceOptions.RESOURCE)
                                    .orElseThrow(
                                            () ->
                                                    new GoogleAdsConnectorException(
                                                            GoogleAdsConnectorErrorCode
                                                                    .INVALID_QUERY,
                                                            "One of resource, query or "
                                                                    + "tables_configs is "
                                                                    + "required"));
            if (fields == null || fields.isEmpty()) {
                throw new GoogleAdsConnectorException(
                        GoogleAdsConnectorErrorCode.INVALID_QUERY,
                        "fields must be a non-empty list when using resource mode");
            }
            fieldPaths = new ArrayList<>();
            for (String field : fields) {
                fieldPaths.add(validateFieldPath(field.trim()));
            }
            StringBuilder sb =
                    new StringBuilder("SELECT ")
                            .append(String.join(", ", fieldPaths))
                            .append(" FROM ")
                            .append(resource);
            if (StringUtils.isNotBlank(filter)) {
                sb.append(" WHERE ").append(filter);
            }
            gaql = sb.toString();
        }

        String customerId =
                config.getOptional(GoogleAdsSourceOptions.CUSTOMER_ID)
                        .orElse(params.getCustomerId());
        CatalogTable catalogTable = client.describeFields(database, resource, fieldPaths);
        return new GoogleAdsTableConfig(gaql, resource, customerId, fieldPaths, catalogTable);
    }

    List<GoogleAdsTableConfig> getTableConfigs() {
        return tableConfigs;
    }

    private String validateFieldPath(String fieldPath) {
        if (!FIELD_PATH_PATTERN.matcher(fieldPath).matches()) {
            throw new GoogleAdsConnectorException(
                    GoogleAdsConnectorErrorCode.INVALID_QUERY,
                    "Invalid GAQL field path: '"
                            + fieldPath
                            + "'. Expected dotted snake_case like campaign.id or metrics.clicks.");
        }
        return fieldPath;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return tableConfigs.stream()
                .map(GoogleAdsTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new GoogleAdsSourceReader(params, tableConfigs, readerContext);
    }
}
