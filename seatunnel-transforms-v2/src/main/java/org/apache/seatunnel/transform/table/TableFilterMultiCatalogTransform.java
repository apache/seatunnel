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

package org.apache.seatunnel.transform.table;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogMapTransform;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.table.TableFilterConfig.PLUGIN_NAME;

@Slf4j
public class TableFilterMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

    public TableFilterMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable table, ReadonlyConfig config) {
        TableFilterConfig tableFilterConfig = TableFilterConfig.of(config);
        boolean include;
        if (tableFilterConfig.getDatabasePattern() == null
                && tableFilterConfig.getSchemaPattern() == null
                && tableFilterConfig.getTablePattern() == null) {
            include =
                    TableFilterConfig.PatternMode.INCLUDE.equals(
                            tableFilterConfig.getPatternMode());
        } else {
            include = tableFilterConfig.isMatch(table.getTablePath());
        }
        return new TableFilterTransform(include, table);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> outputTables = new ArrayList<>();
        for (CatalogTable catalogTable : inputCatalogTables) {
            String tableId = catalogTable.getTableId().toTablePath().toString();
            SeaTunnelTransform<SeaTunnelRow> tableTransform = transformMap.get(tableId);

            if (tableTransform instanceof TableFilterTransform) {
                TableFilterTransform tableFilterTransform = (TableFilterTransform) tableTransform;
                if (tableFilterTransform.isInclude()) {
                    outputTables.add(catalogTable);
                } else {
                    log.info("Table {} is filtered out", tableId);
                }
            }
        }

        log.info(
                "Input tables: {}",
                inputCatalogTables.stream()
                        .map(e -> e.getTablePath().getFullName())
                        .collect(Collectors.toList()));
        log.info(
                "Output tables: {}",
                outputTables.stream()
                        .map(e -> e.getTablePath().getFullName())
                        .collect(Collectors.toList()));

        outputCatalogTables = outputTables;
        return outputTables;
    }
}
