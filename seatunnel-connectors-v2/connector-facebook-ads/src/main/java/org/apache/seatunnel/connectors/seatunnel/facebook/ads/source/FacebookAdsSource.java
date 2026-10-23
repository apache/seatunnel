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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.source;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsTableConfig;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FacebookAdsSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private static final String PLUGIN_NAME = "FacebookAds";
    private static final String DEFAULT_DATABASE = "facebook_ads";
    /** Shared by field and resource names: both are snake_case Graph API identifiers. */
    private static final Pattern NAME_PATTERN = Pattern.compile("^[a-z0-9_]+$");

    private static final Pattern AD_ACCOUNT_ID_PATTERN = Pattern.compile("^[0-9]+$");

    /** Query parameters the connector manages itself; user-supplied params must not clash. */
    private static final Set<String> RESERVED_PARAMS =
            new HashSet<>(Arrays.asList("fields", "limit", "after", "filtering", "access_token"));

    private final FacebookAdsParameters params;
    private final List<FacebookAdsTableConfig> tableConfigs;

    public FacebookAdsSource(FacebookAdsParameters params, ReadonlyConfig config) {
        this.params = params;
        this.tableConfigs = buildTableConfigs(config);
    }

    /**
     * Resolves resource/fields/tables_configs into one or more FacebookAdsTableConfig instances.
     * Unlike Google Ads there is no field metadata service, so every column is STRING and the
     * CatalogTable is built locally without any network call at createSource time.
     */
    private List<FacebookAdsTableConfig> buildTableConfigs(ReadonlyConfig config) {
        if (config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            List<Map<String, Object>> tableConfigMaps =
                    config.get(ConnectorCommonOptions.TABLE_CONFIGS);
            if (tableConfigMaps == null || tableConfigMaps.isEmpty()) {
                throw new FacebookAdsConnectorException(
                        FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                        "tables_configs must contain at least one table entry");
            }
            List<FacebookAdsTableConfig> configs = new ArrayList<>();
            for (Map<String, Object> map : tableConfigMaps) {
                ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(map);
                FacebookAdsTableConfig built = buildOneTableConfig(tableConfig);
                String tableId = built.getTableId();
                boolean duplicate = configs.stream().anyMatch(c -> c.getTableId().equals(tableId));
                if (duplicate) {
                    throw new FacebookAdsConnectorException(
                            FacebookAdsConnectorErrorCode.DUPLICATE_RESOURCE,
                            "Duplicate table in tables_configs: " + tableId);
                }
                configs.add(built);
            }
            return configs;
        } else {
            return Collections.singletonList(
                    buildSingleTableConfig(config, DEFAULT_DATABASE, null));
        }
    }

    private FacebookAdsTableConfig buildOneTableConfig(ReadonlyConfig tableConfig) {
        String tablePath =
                tableConfig
                        .getOptional(FacebookAdsSourceOptions.TABLE_PATH)
                        .orElseThrow(
                                () ->
                                        new FacebookAdsConnectorException(
                                                FacebookAdsConnectorErrorCode.INVALID_TABLE_PATH,
                                                "table_path is required in tables_configs"));
        String[] parts = tablePath.split("\\.", 2);
        if (parts.length != 2 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.INVALID_TABLE_PATH,
                    "table_path must be 'database.resource', got: " + tablePath);
        }
        return buildSingleTableConfig(tableConfig, parts[0], parts[1]);
    }

    /**
     * Builds one table from resource + fields [+ filtering/params]. The ordered field list is the
     * single source of truth shared by the schema, the fields query parameter, and row value
     * extraction. expectedResource is non-null only in tables_configs mode, where the table_path
     * names the edge and a per-entry resource option is not consulted.
     */
    private FacebookAdsTableConfig buildSingleTableConfig(
            ReadonlyConfig config, String database, String expectedResource) {
        String resource =
                expectedResource != null
                        ? expectedResource
                        : config.getOptional(FacebookAdsSourceOptions.RESOURCE)
                                .orElseThrow(
                                        () ->
                                                new FacebookAdsConnectorException(
                                                        FacebookAdsConnectorErrorCode
                                                                .INVALID_CONFIG,
                                                        "One of resource or tables_configs is "
                                                                + "required"));
        if (!NAME_PATTERN.matcher(resource).matches()) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                    "Invalid resource: '"
                            + resource
                            + "'. Expected a snake_case ad account edge like campaigns, adsets, "
                            + "ads or insights.");
        }

        List<String> fields = config.getOptional(FacebookAdsSourceOptions.FIELDS).orElse(null);
        if (fields == null || fields.isEmpty()) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                    "fields must be a non-empty list");
        }
        List<String> fieldNames = new ArrayList<>();
        for (String field : fields) {
            fieldNames.add(validateField(field.trim()));
        }

        Map<String, String> extraParams =
                config.getOptional(FacebookAdsSourceOptions.PARAMS).orElse(null);
        if (extraParams != null) {
            for (String key : extraParams.keySet()) {
                if (RESERVED_PARAMS.contains(key)) {
                    throw new FacebookAdsConnectorException(
                            FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                            "params must not contain the reserved key '"
                                    + key
                                    + "'; it is managed by the connector.");
                }
            }
        }

        String filtering = config.getOptional(FacebookAdsSourceOptions.FILTERING).orElse(null);
        String adAccountId =
                normalizeAdAccountId(
                        config.getOptional(FacebookAdsSourceOptions.AD_ACCOUNT_ID)
                                .orElse(params.getAdAccountId()));

        CatalogTable catalogTable = buildCatalogTable(database, resource, fieldNames);
        return new FacebookAdsTableConfig(
                resource, adAccountId, fieldNames, filtering, extraParams, catalogTable);
    }

    /**
     * The Graph API has no field metadata endpoint, so every column is STRING: Facebook already
     * returns most metrics as JSON strings, and nested objects/arrays are emitted as JSON text.
     */
    private CatalogTable buildCatalogTable(
            String database, String resource, List<String> fieldNames) {
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        for (String fieldName : fieldNames) {
            schemaBuilder.column(
                    PhysicalColumn.of(
                            fieldName, BasicType.STRING_TYPE, null, null, true, null, null));
        }
        return CatalogTable.of(
                TableIdentifier.of(PLUGIN_NAME, database, resource),
                schemaBuilder.build(),
                Collections.emptyMap(),
                Collections.emptyList(),
                "");
    }

    private String normalizeAdAccountId(String adAccountId) {
        if (StringUtils.isBlank(adAccountId)) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG, "ad_account_id is required");
        }
        String normalized = adAccountId.startsWith("act_") ? adAccountId.substring(4) : adAccountId;
        if (!AD_ACCOUNT_ID_PATTERN.matcher(normalized).matches()) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                    "Invalid ad_account_id: '"
                            + adAccountId
                            + "'. Expected digits, optionally prefixed with act_.");
        }
        return normalized;
    }

    private String validateField(String field) {
        if (!NAME_PATTERN.matcher(field).matches()) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.INVALID_CONFIG,
                    "Invalid field name: '"
                            + field
                            + "'. Expected snake_case like id, name or campaign_id.");
        }
        return field;
    }

    List<FacebookAdsTableConfig> getTableConfigs() {
        return tableConfigs;
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
                .map(FacebookAdsTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new FacebookAdsSourceReader(params, tableConfigs, readerContext);
    }
}
