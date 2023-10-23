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

package org.apache.seatunnel.transform.catalogtablemeta;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CatalogTableMetaTransform implements SeaTunnelTransform<SeaTunnelRow> {
    public static final String PLUGIN_NAME = "CatalogTableMeta";

    private final List<CatalogTable> inputCatalogTables;
    private final Map<String, CatalogTable> outputCatalogTables;

    public CatalogTableMetaTransform(List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        this.inputCatalogTables = inputCatalogTables;
        this.outputCatalogTables = new LinkedHashMap<>();
        inputCatalogTables.forEach(
                in -> {
                    CatalogTableMetaConfig catalogConf = CatalogTableMetaConfig.of(config, in);
                    TableSchema inTableSchema = in.getTableSchema();
                    TableSchema.Builder builder = TableSchema.builder();
                    builder.columns(inTableSchema.getColumns());
                    if (catalogConf.getPrimaryKey() != null) {
                        builder.primaryKey(catalogConf.getPrimaryKey());
                    }
                    if (catalogConf.getConstraintKeys() != null) {
                        builder.constraintKey(catalogConf.getConstraintKeys());
                    }

                    TableSchema outTableSchema = builder.build();
                    String catalogName = in.getTableId().getCatalogName();
                    TablePath tablePath = in.getTableId().toTablePath();
                    if (catalogConf.getTablePath() != null) {
                        tablePath = TablePath.of(catalogConf.getTablePath());
                    }
                    if (catalogConf.getCatalogName() != null) {
                        catalogName = catalogConf.getCatalogName();
                    }
                    TableIdentifier outTableIdentifier = TableIdentifier.of(catalogName, tablePath);
                    Map<String, String> outOptions = new HashMap<>();
                    if (catalogConf.getOptions() != null) {
                        outOptions.putAll(catalogConf.getOptions());
                    }
                    List<String> outPartitionKeys = Arrays.asList(catalogConf.getPartitionKeys());
                    String outComment = catalogConf.getComment();
                    String outCatalogName = catalogConf.getCatalogName();

                    CatalogTable out =
                            CatalogTable.of(
                                    outTableIdentifier,
                                    outTableSchema,
                                    outOptions,
                                    outPartitionKeys,
                                    outComment,
                                    outCatalogName);

                    outputCatalogTables.put(in.getTableId().toTablePath().toString(), out);
                });
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {}

    @Override
    public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {}

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return outputCatalogTables
                .values()
                .iterator()
                .next()
                .getTableSchema()
                .toPhysicalRowDataType();
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        return outputCatalogTables.values().iterator().next();
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return new ArrayList<>(outputCatalogTables.values());
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        if (StringUtils.isNotEmpty(row.getTableId())) {
            row.setTableId(
                    outputCatalogTables
                            .get(row.getTableId())
                            .getTableId()
                            .toTablePath()
                            .toString());
        }
        return row;
    }
}
