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

package org.apache.seatunnel.connectors.seatunnel.iceberg.data;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.util.SeaTunnelSchemaUtil;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;

public class RowDataProjection {
    /**
     * Creates a projecting wrapper for {@link SeaTunnelRow} rows.
     *
     * <p>This projection will not project the nested children types of repeated types like lists
     * and maps.
     *
     * @param schema schema of rows wrapped by this projection
     * @param projectedSchema result schema of the projected rows
     * @return a wrapper to project rows
     */
    public static RowDataProjection create(Schema schema, Schema projectedSchema) {
        return RowDataProjection.create(
                SeaTunnelSchemaUtil.convert(schema), schema.asStruct(), projectedSchema.asStruct());
    }

    /**
     * Creates a projecting wrapper for {@link SeaTunnelRow} rows.
     *
     * <p>This projection will not project the nested children types of repeated types like lists
     * and maps.
     *
     * @param rowType seaTunnel row type of rows wrapped by this projection
     * @param schema schema of rows wrapped by this projection
     * @param projectedSchema result schema of the projected rows
     * @return a wrapper to project rows
     */
    public static RowDataProjection create(
            SeaTunnelRowType rowType, Types.StructType schema, Types.StructType projectedSchema) {
        return new RowDataProjection(rowType, schema, projectedSchema);
    }

    private RowDataProjection(
            SeaTunnelRowType rowType, Types.StructType rowStruct, Types.StructType projectType) {
        this.rowType = rowType;
        this.rowStruct = rowStruct;
        this.projectType = projectType;
    }

    private final SeaTunnelRowType rowType;
    private final Types.StructType rowStruct;
    private final Types.StructType projectType;

    public SeaTunnelRow wrap(SeaTunnelRow row) {

        checkArgument(row != null, "Invalid row data: null");

        List<String> ProjectionNames =
                projectType.fields().stream()
                        .map(Types.NestedField::name)
                        .collect(Collectors.toList());

        int[] pkFields = ProjectionNames.stream().mapToInt(rowType::indexOf).toArray();

        Object[] fields = new Object[pkFields.length];
        for (int i = 0; i < pkFields.length; i++) {
            fields[i] = row.getField(pkFields[i]);
        }
        SeaTunnelRow newRow = new SeaTunnelRow(fields);
        newRow.setTableId(row.getTableId());
        newRow.setRowKind(row.getRowKind());
        return newRow;
    }
}
