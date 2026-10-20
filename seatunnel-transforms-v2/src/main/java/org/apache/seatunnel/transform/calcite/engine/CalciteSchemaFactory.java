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

package org.apache.seatunnel.transform.calcite.engine;

import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.schema.SchemaPlus;

import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.calcite.adapter.SeaTunnelScannableTable;
import org.apache.seatunnel.transform.calcite.type.CalciteTypeConverter;

import lombok.experimental.UtilityClass;

import java.util.ArrayList;
import java.util.List;

/**
 * Registers a SeaTunnel table (by name and row-type) into a Calcite {@link SchemaPlus} as a {@link
 * SeaTunnelScannableTable}.
 */
@UtilityClass
public final class CalciteSchemaFactory {

    /**
     * Registers a table with the given name into the Calcite schema.
     *
     * @return the created {@link SeaTunnelScannableTable} so the caller can inject rows later
     */
    public static SeaTunnelScannableTable registerTable(
            SchemaPlus schema,
            String tableName,
            SeaTunnelRowType seaTunnelRowType,
            RelDataTypeFactory typeFactory) {

        String[] fieldNames = seaTunnelRowType.getFieldNames();
        List<String> names = new ArrayList<>(fieldNames.length);
        List<RelDataType> types = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            names.add(fieldNames[i]);
            RelDataType fieldType =
                    CalciteTypeConverter.toCalciteType(
                            typeFactory, seaTunnelRowType.getFieldType(i));
            types.add(typeFactory.createTypeWithNullability(fieldType, true));
        }
        RelDataType rowType = typeFactory.createStructType(types, names);

        SeaTunnelScannableTable table = new SeaTunnelScannableTable(rowType);
        schema.add(tableName, table);
        return table;
    }
}
