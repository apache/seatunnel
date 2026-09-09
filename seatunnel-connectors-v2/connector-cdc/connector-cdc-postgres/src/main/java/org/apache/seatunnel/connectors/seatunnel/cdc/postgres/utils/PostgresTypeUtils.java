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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.utils;

import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import io.debezium.relational.Column;

public class PostgresTypeUtils {
    private PostgresTypeUtils() {}

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
        return PostgresTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    /** Convert a pgoutput RELATION column with the metadata required by schema evolution. */
    public static BasicTypeDefine convertRelationColumnToTypeDefine(Column column) {
        return BasicTypeDefine.builder()
                .name(column.name())
                .columnType(column.typeName())
                .dataType(column.typeName())
                .sqlType(column.jdbcType())
                .length(column.length() < 0 ? null : (long) column.length())
                .precision(column.length() < 0 ? null : (long) column.length())
                .scale(column.scale().orElse(0))
                .nullable(column.isOptional())
                .defaultValue(column.defaultValueExpression().orElse(null))
                .comment(column.comment())
                .build();
    }
}
