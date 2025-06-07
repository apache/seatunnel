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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils;

import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter;

import io.debezium.relational.Column;

import java.util.Optional;

/** Utilities for converting from oracle types to SeaTunnel types. */
public class OracleTypeUtils {

    public static SeaTunnelDataType<?> convertFromColumn(Column column) {
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(column.name())
                        .columnType(column.typeName())
                        .dataType(column.typeName())
                        .length((long) column.length())
                        .precision((long) column.length())
                        .scale(column.scale().orElse(0))
                        .build();
        org.apache.seatunnel.api.table.catalog.Column seaTunnelColumn =
                OracleTypeConverter.INSTANCE.convert(typeDefine);
        return seaTunnelColumn.getDataType();
    }

    public static org.apache.seatunnel.api.table.catalog.Column convertToSeaTunnelColumn(
            io.debezium.relational.Column column) {

        Optional<String> defaultValueExpression = column.defaultValueExpression();
        Object defaultValue = defaultValueExpression.orElse(null);

        BasicTypeDefine.BasicTypeDefineBuilder<Object> builder =
                BasicTypeDefine.builder()
                        .name(column.name())
                        .columnType(column.typeName())
                        .dataType(column.typeName())
                        .scale(column.scale().orElse(0))
                        .nullable(column.isOptional())
                        .defaultValue(defaultValue);

        // The default value of length in column is -1 if it is not set
        if (column.length() >= 0) {
            builder.length((long) column.length()).precision((long) column.length());
        }

        // TIMESTAMP or TIMESTAMP WITH TIME ZONE
        // This is useful for OracleTypeConverter.convert()
        if (column.typeName() != null && column.typeName().toUpperCase().startsWith("TIMESTAMP")) {
            builder.scale(column.length());
        }

        return new OracleTypeConverter(false, false).convert(builder.build());
    }
}
