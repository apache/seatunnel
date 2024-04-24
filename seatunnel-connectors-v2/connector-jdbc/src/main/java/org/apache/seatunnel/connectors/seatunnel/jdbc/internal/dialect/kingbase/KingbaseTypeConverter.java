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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresTypeConverter;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

// reference https://help.kingbase.com.cn/v8/development/sql-plsql/sql/datatype.html#id2
@Slf4j
@AutoService(TypeConverter.class)
public class KingbaseTypeConverter extends PostgresTypeConverter {
    public static final String KB_TINYINT = "TINYINT";
    public static final String KB_MONEY = "MONEY";
    public static final String KB_BLOB = "BLOB";
    public static final String KB_CLOB = "CLOB";
    public static final String KB_BIT = "BIT";

    public static final KingbaseTypeConverter INSTANCE = new KingbaseTypeConverter();

    @Override
    public String identifier() {
        return DatabaseIdentifier.KINGBASE;
    }

    @Override
    public Column convert(BasicTypeDefine typeDefine) {
        try {
            return super.convert(typeDefine);
        } catch (SeaTunnelRuntimeException e) {
            PhysicalColumn.PhysicalColumnBuilder builder =
                    PhysicalColumn.builder()
                            .name(typeDefine.getName())
                            .sourceType(typeDefine.getColumnType())
                            .nullable(typeDefine.isNullable())
                            .defaultValue(typeDefine.getDefaultValue())
                            .comment(typeDefine.getComment());

            String kingbaseDataType = typeDefine.getDataType().toUpperCase();
            switch (kingbaseDataType) {
                case KB_TINYINT:
                    builder.dataType(BasicType.BYTE_TYPE);
                    break;
                case KB_MONEY:
                    builder.dataType(new DecimalType(38, 18));
                    builder.columnLength(38L);
                    builder.scale(18);
                    break;
                case KB_BLOB:
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    builder.columnLength((long) (1024 * 1024 * 1024));
                    break;
                case KB_CLOB:
                    builder.dataType(BasicType.STRING_TYPE);
                    builder.columnLength(typeDefine.getLength());
                    builder.columnLength((long) (1024 * 1024 * 1024));
                    break;
                case KB_BIT:
                    builder.dataType(PrimitiveByteArrayType.INSTANCE);
                    // BIT(M) -> BYTE(M/8)
                    long byteLength = typeDefine.getLength() / 8;
                    byteLength += typeDefine.getLength() % 8 > 0 ? 1 : 0;
                    builder.columnLength(byteLength);
                    break;
                default:
                    throw CommonError.convertToSeaTunnelTypeError(
                            DatabaseIdentifier.KINGBASE,
                            typeDefine.getDataType(),
                            typeDefine.getName());
            }
            return builder.build();
        }
    }

    @Override
    public BasicTypeDefine reconvert(Column column) {
        try {
            return super.reconvert(column);
        } catch (SeaTunnelRuntimeException e) {
            throw CommonError.convertToConnectorTypeError(
                    DatabaseIdentifier.KINGBASE,
                    column.getDataType().getSqlType().name(),
                    column.getName());
        }
    }
}
