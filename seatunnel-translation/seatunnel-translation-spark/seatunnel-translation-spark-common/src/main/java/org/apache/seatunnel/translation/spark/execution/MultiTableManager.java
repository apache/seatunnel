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
package org.apache.seatunnel.translation.spark.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.Handover;
import org.apache.seatunnel.translation.spark.serialization.InternalMultiRowCollector;
import org.apache.seatunnel.translation.spark.serialization.InternalRowCollector;
import org.apache.seatunnel.translation.spark.serialization.InternalRowConverter;
import org.apache.seatunnel.translation.spark.utils.TypeConverterUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
public class MultiTableManager implements Serializable {

    private Map<String, InternalRowConverter> rowSerializationMap;

    private InternalRowConverter rowSerialization;
    private CatalogTable mergeCatalogTable;
    private boolean isMultiTable = false;

    public MultiTableManager(CatalogTable[] catalogTables) {
        List<ColumnWithIndex> columnWithIndexes = mergeSchema(catalogTables);
        if (catalogTables.length > 1) {
            isMultiTable = true;
            rowSerializationMap =
                    columnWithIndexes.stream()
                            .collect(
                                    Collectors.toMap(
                                            columnWithIndex ->
                                                    columnWithIndex
                                                            .getCatalogTable()
                                                            .getTablePath()
                                                            .toString(),
                                            columnWithIndex ->
                                                    new InternalRowConverter(
                                                            mergeCatalogTable.getSeaTunnelRowType(),
                                                            columnWithIndex.getIndex())));
        } else {
            rowSerialization = new InternalRowConverter(catalogTables[0].getSeaTunnelRowType());
        }
        log.info("Multi-table enabled:{}", isMultiTable);
        log.info(
                "merged table {}, schema {}",
                mergeCatalogTable.getTablePath(),
                mergeCatalogTable.getSeaTunnelRowType());
        for (ColumnWithIndex columnWithIndex : columnWithIndexes) {
            log.info("MultiTableManager columnWithIndex:{}", columnWithIndex);
        }
    }

    public SeaTunnelRow reconvert(InternalRow record) throws IOException {
        if (isMultiTable) {
            String tableId = record.getString(1);
            return rowSerializationMap.get(tableId).reconvert(record);
        }
        return rowSerialization.reconvert(record);
    }

    public StructType getTableSchema() {
        return (StructType) TypeConverterUtils.parcel(mergeCatalogTable.getSeaTunnelRowType());
    }

    public List<ColumnWithIndex> mergeSchema(CatalogTable[] catalogTables) {
        List<ColumnWithIndex> columnWithIndexes = new ArrayList<>();
        if (catalogTables.length == 1) {
            CatalogTable catalogTable = catalogTables[0];
            columnWithIndexes.add(
                    new ColumnWithIndex(
                            IntStream.rangeClosed(
                                            0, catalogTable.getSeaTunnelRowType().getTotalFields())
                                    .toArray(),
                            catalogTable));
            mergeCatalogTable = catalogTable;
            return columnWithIndexes;
        }
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();
        int indexSize = -1;
        HashMap<SeaTunnelDataType<?>, IndexQueue<Integer>> map = new HashMap<>();
        for (int i = 0; i < catalogTables.length; i++) {
            CatalogTable catalogTable = catalogTables[i];
            SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
            SeaTunnelDataType<?>[] seaTunnelDataTypes = seaTunnelRowType.getFieldTypes();
            int[] indexes = new int[seaTunnelDataTypes.length];
            for (int j = 0; j < seaTunnelDataTypes.length; j++) {
                IndexQueue<Integer> indexQueue =
                        map.computeIfAbsent(
                                seaTunnelDataTypes[j], k -> new IndexQueue<>(new ArrayList<>()));
                if (indexQueue.hasNext()) {
                    indexes[j] = indexQueue.next();
                } else {
                    indexSize++;
                    indexes[j] = indexSize;
                    indexQueue.add(indexSize);
                    fieldNames.add(editColumnName(indexSize));
                    fieldTypes.add(seaTunnelDataTypes[j]);
                }
            }
            map.forEach((k, v) -> v.reset());
            columnWithIndexes.add(new ColumnWithIndex(indexes, catalogTable));
        }
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType[0]));
        mergeCatalogTable =
                CatalogTableUtil.getCatalogTable(
                        "spark", "default", "default", "merge_table", rowType);
        return columnWithIndexes;
    }

    public static String editColumnName(int index) {
        return "column" + index;
    }

    public InternalRowCollector getInternalRowCollector(
            Handover<InternalRow> handover,
            Object checkpointLock,
            Map<String, String> envOptionsInfo) {
        if (isMultiTable) {
            return new InternalMultiRowCollector(
                    handover, checkpointLock, rowSerializationMap, envOptionsInfo);
        } else {
            return new InternalRowCollector(
                    handover, checkpointLock, rowSerialization, envOptionsInfo);
        }
    }
}
