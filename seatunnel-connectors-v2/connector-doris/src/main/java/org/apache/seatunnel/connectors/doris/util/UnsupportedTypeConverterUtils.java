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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SqlType;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.type.BasicType.DOUBLE_TYPE;

public class UnsupportedTypeConverterUtils {
    public static Object convertBigDecimal(BigDecimal bigDecimal) {
        if (bigDecimal.precision() > 38) {
            return bigDecimal.doubleValue();
        }
        return bigDecimal;
    }

    public static SeaTunnelRow convertRow(SeaTunnelRow row) {
        List<Object> newValues =
                Arrays.stream(row.getFields())
                        .map(
                                value -> {
                                    if (value instanceof BigDecimal) {
                                        return convertBigDecimal((BigDecimal) value);
                                    }
                                    return value;
                                })
                        .collect(Collectors.toList());
        return new SeaTunnelRow(newValues.toArray());
    }

    public static CatalogTable convertCatalogTable(CatalogTable catalogTable) {
        TableSchema tableSchema = catalogTable.getTableSchema();
        List<Column> columns = tableSchema.getColumns();
        List<Column> newColumns =
                columns.stream()
                        .map(
                                column -> {
                                    if (column.getDataType().getSqlType().equals(SqlType.DECIMAL)) {
                                        DecimalType decimalType =
                                                (DecimalType) column.getDataType();
                                        if (decimalType.getPrecision() > 38) {
                                            return PhysicalColumn.of(
                                                    column.getName(),
                                                    DOUBLE_TYPE,
                                                    22,
                                                    column.isNullable(),
                                                    null,
                                                    column.getComment(),
                                                    "DOUBLE",
                                                    false,
                                                    false,
                                                    0L,
                                                    column.getOptions(),
                                                    22L);
                                        }
                                    }
                                    return column;
                                })
                        .collect(Collectors.toList());
        TableSchema newtableSchema =
                TableSchema.builder()
                        .columns(newColumns)
                        .primaryKey(tableSchema.getPrimaryKey())
                        .constraintKey(tableSchema.getConstraintKeys())
                        .build();

        return CatalogTable.of(
                catalogTable.getTableId(),
                newtableSchema,
                catalogTable.getOptions(),
                catalogTable.getPartitionKeys(),
                catalogTable.getComment(),
                catalogTable.getCatalogName());
    }
}
