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

package org.apache.seatunnel.connectors.seatunnel.salesforce.source;

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
import org.apache.seatunnel.connectors.seatunnel.salesforce.client.SalesforceClient;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceParameters;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.salesforce.exception.SalesforceConnectorException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SalesforceSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private static final String PLUGIN_NAME = "Salesforce";

    private final SalesforceParameters params;
    private final List<SalesforceTableConfig> tableConfigs;

    public SalesforceSource(SalesforceParameters params, ReadonlyConfig config) {
        this.params = params;
        this.tableConfigs = buildTableConfigs(params, config);
    }

    /**
     * Resolves the user-facing object_name / tables_configs options into one or more
     * SalesforceTableConfig instances, calling /describe per object to derive the CatalogTable
     * schema. Runs once during factory createSource and uses a one-shot SalesforceClient whose
     * lifetime is scoped to this call (try-with-resources).
     */
    private List<SalesforceTableConfig> buildTableConfigs(
            SalesforceParameters params, ReadonlyConfig config) {
        try (SalesforceClient client = new SalesforceClient(params)) {
            client.authenticate();

            if (config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
                List<Map<String, Object>> tableConfigMaps =
                        config.get(ConnectorCommonOptions.TABLE_CONFIGS);
                List<SalesforceTableConfig> configs = new ArrayList<>();
                for (Map<String, Object> map : tableConfigMaps) {
                    ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(map);
                    SalesforceTableConfig built = buildOneTableConfig(tableConfig, client);
                    String tableId = built.getTableId();
                    boolean duplicate =
                            configs.stream().anyMatch(c -> c.getTableId().equals(tableId));
                    if (duplicate) {
                        throw new SalesforceConnectorException(
                                SalesforceConnectorErrorCode.DUPLICATE_OBJECT,
                                "Duplicate object in tables_configs: " + tableId);
                    }
                    configs.add(built);
                }
                return configs;
            } else {
                String objectName = config.get(SalesforceSourceOptions.OBJECT_NAME);
                String soql = buildSoql(config, objectName);
                CatalogTable table = client.describeObject(PLUGIN_NAME, objectName);
                return Collections.singletonList(
                        new SalesforceTableConfig(soql, objectName, table));
            }
        } catch (SalesforceConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new SalesforceConnectorException(
                    SalesforceConnectorErrorCode.DESCRIBE_OBJECT_FAILED,
                    "Failed to build Salesforce table configs",
                    e);
        }
    }

    private SalesforceTableConfig buildOneTableConfig(
            ReadonlyConfig tableConfig, SalesforceClient client) {
        String tablePath =
                tableConfig
                        .getOptional(SalesforceSourceOptions.TABLE_PATH)
                        .orElseThrow(
                                () ->
                                        new SalesforceConnectorException(
                                                SalesforceConnectorErrorCode.INVALID_TABLE_PATH,
                                                "table_path is required in tables_configs"));
        String[] parts = tablePath.split("\\.", 2);
        if (parts.length != 2 || StringUtils.isBlank(parts[1])) {
            throw new SalesforceConnectorException(
                    SalesforceConnectorErrorCode.INVALID_TABLE_PATH,
                    "table_path must be 'database.ObjectName', got: " + tablePath);
        }
        String database = parts[0];
        String objectName = parts[1];
        String soql = buildSoql(tableConfig, objectName);
        CatalogTable table = client.describeObject(database, objectName);
        return new SalesforceTableConfig(soql, objectName, table);
    }

    /**
     * Builds the SOQL the Bulk API job will run for one object. Always selects every
     * describe-discovered field via FIELDS(ALL) so the emitted rows stay positionally aligned with
     * the CatalogTable schema. An optional filter is appended verbatim as the WHERE clause.
     */
    private String buildSoql(ReadonlyConfig config, String objectName) {
        StringBuilder soql = new StringBuilder("SELECT FIELDS(ALL) FROM ").append(objectName);
        String filter = config.getOptional(SalesforceSourceOptions.FILTER).orElse(null);
        if (filter != null) {
            soql.append(" WHERE ").append(filter);
        }
        return soql.toString();
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
                .map(SalesforceTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new SalesforceSourceReader(params, tableConfigs, readerContext);
    }
}
