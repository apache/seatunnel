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

import org.apache.commons.lang3.tuple.Pair;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class TableMergeMultiCatalogTransform extends AbstractMultiCatalogMapTransform {

    public TableMergeMultiCatalogTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public String getPluginName() {
        return TableMergeTransform.PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> buildTransform(
            CatalogTable table, ReadonlyConfig config) {
        return new TableMergeTransform(TableMergeConfig.of(config), table);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> outputTables = new ArrayList<>();
        LinkedHashMap<String, List<Pair<CatalogTable, CatalogTable>>> mergeTables =
                new LinkedHashMap<>();
        for (int i = 0; i < outputCatalogTables.size(); i++) {
            CatalogTable inputTable = inputCatalogTables.get(i);
            CatalogTable outputTable = outputCatalogTables.get(i);

            String tableId = outputTable.getTablePath().getFullName();
            SeaTunnelTransform<SeaTunnelRow> transform = transformMap.get(tableId);
            if (transform instanceof IdentityTransform) {
                outputTables.add(outputTable);
            } else {
                if (!mergeTables.containsKey(tableId)) {
                    mergeTables.put(tableId, new ArrayList<>());
                }
                mergeTables.get(tableId).add(Pair.of(inputTable, outputTable));
            }
        }

        // validate
        for (String key : mergeTables.keySet()) {
            List<Pair<CatalogTable, CatalogTable>> tables = mergeTables.get(key);
            Pair<CatalogTable, CatalogTable> firstTable = tables.get(0);

            tables.stream()
                    .allMatch(
                            other -> {
                                boolean match =
                                        firstTable
                                                .getRight()
                                                .getSeaTunnelRowType()
                                                .equals(other.getRight().getSeaTunnelRowType());
                                if (!match) {
                                    throw new UnsupportedOperationException(
                                            "TableMergeTransform: "
                                                    + "The schema of the tables to be merged must be the same. "
                                                    + "The schema of the table "
                                                    + firstTable
                                                            .getLeft()
                                                            .getTablePath()
                                                            .getFullName()
                                                    + " is different from the schema of the table "
                                                    + other.getLeft().getTablePath().getFullName());
                                }
                                return match;
                            });
            outputTables.add(firstTable.getRight());
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

        return outputTables;
    }
}
