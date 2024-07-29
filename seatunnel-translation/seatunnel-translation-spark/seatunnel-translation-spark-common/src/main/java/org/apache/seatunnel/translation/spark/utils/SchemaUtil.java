package org.apache.seatunnel.translation.spark.utils;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.translation.spark.execution.ColumnWithIndex;
import org.apache.seatunnel.translation.spark.execution.IndexQueue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SchemaUtil {

    public static ColumnWithIndex[] mergeSchema(List<CatalogTable> catalogTables) {
        if (catalogTables.size() == 1) {
            CatalogTable catalogTable = catalogTables.get(0);
            return new ColumnWithIndex[] {
                new ColumnWithIndex(
                        IntStream.rangeClosed(
                                        0, catalogTable.getSeaTunnelRowType().getTotalFields())
                                .toArray(),
                        catalogTable,
                        null)
            };
        }
        List<String> fieldNames = new ArrayList<>();
        List<SeaTunnelDataType<?>> fieldTypes = new ArrayList<>();
        List<ColumnWithIndex> columnWithIndexes = new ArrayList<>();
        fieldTypes.sort(Comparator.comparingInt(o -> o.getSqlType().ordinal()));
        int indexSize = -1;
        HashMap<SeaTunnelDataType<?>, IndexQueue<Integer>> map = new HashMap<>();
        for (int i = 0; i < catalogTables.size(); i++) {
            CatalogTable catalogTable = catalogTables.get(i);
            SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
            SeaTunnelDataType<?>[] seaTunnelDataTypes = seaTunnelRowType.getFieldTypes();
            int[] indexes = new int[seaTunnelDataTypes.length];
            if (i == 0) {
                fieldNames.addAll(
                        IntStream.range(0, seaTunnelRowType.getTotalFields())
                                .mapToObj(SchemaUtil::editColumnName)
                                .collect(Collectors.toList()));
                fieldTypes.addAll(Arrays.asList(seaTunnelRowType.getFieldTypes()));
                indexes = IntStream.range(0, seaTunnelRowType.getTotalFields()).toArray();
                columnWithIndexes.add(new ColumnWithIndex(indexes, catalogTable, null));
                indexSize += seaTunnelRowType.getTotalFields();
                for (int q = 0; q < seaTunnelDataTypes.length; q++) {
                    map.computeIfAbsent(
                                    seaTunnelDataTypes[q], k -> new IndexQueue<>(new ArrayList<>()))
                            .add(indexes[q]);
                }
                map.forEach((k, v) -> v.reset());
                continue;
            }
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
            columnWithIndexes.add(new ColumnWithIndex(indexes, catalogTable, null));
        }
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        fieldNames.toArray(new String[0]),
                        fieldTypes.toArray(new SeaTunnelDataType[0]));
        CatalogTable mergeCatalogTable =
                CatalogTableUtil.getCatalogTable(
                        "spark", "default", "default", "merge_table", rowType);
        columnWithIndexes.get(0).setMergeCatalogTable(mergeCatalogTable);
        return columnWithIndexes.toArray(new ColumnWithIndex[0]);
    }

    public static String editColumnName(int index) {
        return "column" + index;
    }
}
