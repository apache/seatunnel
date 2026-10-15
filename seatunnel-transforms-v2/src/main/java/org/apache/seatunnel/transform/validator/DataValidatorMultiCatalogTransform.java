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

package org.apache.seatunnel.transform.validator;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogMapTransform;
import org.apache.seatunnel.transform.common.IdentityMapTransform;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Multi-table wrapper of the {@link DataValidatorTransform}: every table matched by the
 * transform-level rule (or selected by a {@code table_transform} entry) is validated with its own
 * field rules, tables without a matching rule are passed through unchanged.
 */
public class DataValidatorMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

    /**
     * Error handling policies of the transform. They are configured once at the transform level and
     * have to apply to every validated table, including the tables that are built from a {@code
     * table_transform} entry, which replaces the rest of the transform level rule.
     */
    private static final List<Option<?>> GLOBAL_POLICY_OPTIONS =
            Arrays.asList(
                    TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION,
                    TransformCommonOptions.ERROR_TABLE_OPTION);

    public DataValidatorMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public String getPluginName() {
        return DataValidatorTransform.PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return new DataValidatorTransform(withGlobalPolicies(config), inputCatalogTable);
    }

    /**
     * The error handling policy is transform level configuration, so a table selected by a {@code
     * table_transform} entry inherits it unless the entry sets its own value.
     */
    private ReadonlyConfig withGlobalPolicies(ReadonlyConfig tableConfig) {
        Map<String, Object> mergedOptions = new HashMap<>(tableConfig.getSourceMap());
        for (Option<?> option : GLOBAL_POLICY_OPTIONS) {
            if (tableConfig.getOptional(option).isEmpty()) {
                transformLevelConfig
                        .getOptional(option)
                        .ifPresent(value -> mergedOptions.put(option.key(), value));
            }
        }
        return ReadonlyConfig.fromMap(mergedOptions);
    }

    /** Mirrors the mandatory options of the transform-level rule declared by the factory. */
    @Override
    protected boolean hasTransformLevelRuleConfigured(ReadonlyConfig config) {
        return hasAnyOptionConfigured(config, DataValidatorTransformConfig.FIELD_RULES);
    }

    /**
     * {@link DataValidatorTransform} produces the validated table plus an error table when {@code
     * row_error_handle_way = ROUTE_TO_TABLE} is used. The error table schema has to reach the sink,
     * otherwise the routed rows are written with the row type of the validated table and the
     * assertions of the error table fail. The base wrapper only keeps the validated tables, so the
     * extra tables of the inner transforms are appended here.
     */
    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> producedTables = new ArrayList<>(super.getProducedCatalogTables());
        Set<String> producedTablePaths = new HashSet<>();
        for (CatalogTable producedTable : producedTables) {
            producedTablePaths.add(producedTable.getTableId().toTablePath().toString());
        }
        for (SeaTunnelTransform<SeaTunnelRow> transform : transformMap.values()) {
            List<CatalogTable> innerProducedTables = transform.getProducedCatalogTables();
            for (int i = 1; i < innerProducedTables.size(); i++) {
                CatalogTable extraTable = innerProducedTables.get(i);
                if (producedTablePaths.add(extraTable.getTableId().toTablePath().toString())) {
                    producedTables.add(extraTable);
                }
            }
        }
        return producedTables;
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(CatalogTable catalogTable) {
        return new IdentityMapTransform(catalogTable);
    }
}
