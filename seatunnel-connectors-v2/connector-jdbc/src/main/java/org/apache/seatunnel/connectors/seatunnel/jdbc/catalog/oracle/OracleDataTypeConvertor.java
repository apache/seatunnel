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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleTypeConverter;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** @deprecated instead by {@link OracleTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class OracleDataTypeConvertor implements DataTypeConvertor<String> {

    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Long DEFAULT_PRECISION = 38L;
    public static final Integer DEFAULT_SCALE = 18;

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "Oracle Type cannot be null");

        Long precision = null;
        Integer scale = null;
        switch (connectorDataType) {
            case OracleTypeConverter.ORACLE_NUMBER:
                precision = MapUtils.getLong(dataTypeProperties, PRECISION, DEFAULT_PRECISION);
                scale = MapUtils.getInteger(dataTypeProperties, SCALE, DEFAULT_SCALE);
                break;
            default:
                break;
        }

        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(field)
                        .columnType(connectorDataType)
                        .dataType(normalizeTimestamp(connectorDataType))
                        .length(precision)
                        .precision(precision)
                        .scale(scale)
                        .build();

        return OracleTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType cannot be null");

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

        BasicTypeDefine typeDefine = OracleTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }

    public static String normalizeTimestamp(String oracleType) {
        // Create a pattern to match TIMESTAMP followed by an optional (0-9)
        String pattern = "^TIMESTAMP(\\([0-9]\\))?$";
        // Create a Pattern object
        Pattern r = Pattern.compile(pattern);
        // Now create matcher object.
        Matcher m = r.matcher(oracleType);
        if (m.find()) {
            return "TIMESTAMP";
        } else {
            return oracleType;
        }
    }

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.ORACLE;
    }
}
