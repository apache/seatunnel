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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerTypeConverter;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.google.auto.service.AutoService;
import lombok.NonNull;

import java.util.Map;

/** @deprecated instead by {@link SqlServerTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class SqlServerDataTypeConvertor implements DataTypeConvertor<SqlServerType> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final String LENGTH = "length";
    public static final Integer DEFAULT_PRECISION = 10;
    public static final Integer DEFAULT_SCALE = 0;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, @NonNull String connectorDataType) {
        Pair<SqlServerType, Map<String, Object>> sqlServerType =
                SqlServerType.parse(connectorDataType);
        return toSeaTunnelType(field, sqlServerType.getLeft(), sqlServerType.getRight());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field,
            @NonNull SqlServerType connectorDataType,
            Map<String, Object> dataTypeProperties) {
        int precision =
                Integer.parseInt(
                        dataTypeProperties.getOrDefault(PRECISION, DEFAULT_PRECISION).toString());
        long length = Long.parseLong(dataTypeProperties.getOrDefault(LENGTH, 0).toString());
        int scale = (int) dataTypeProperties.getOrDefault(SCALE, DEFAULT_SCALE);
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(field)
                        .columnType(connectorDataType.getSqlTypeName())
                        .dataType(connectorDataType.getSqlTypeName())
                        .length(length)
                        .precision((long) precision)
                        .scale(scale)
                        .build();

        return SqlServerTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public SqlServerType toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        Long precision = MapUtils.getLong(dataTypeProperties, PRECISION);
        Integer scale = MapUtils.getInteger(dataTypeProperties, SCALE);

        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(seaTunnelDataType)
                        .columnLength(precision)
                        .scale(scale)
                        .nullable(true)
                        .build();

        BasicTypeDefine typeDefine = SqlServerTypeConverter.INSTANCE.reconvert(column);
        return SqlServerType.parse(typeDefine.getColumnType()).getLeft();
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.SQLSERVER;
    }
}
