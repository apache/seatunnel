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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter;

import org.apache.commons.collections4.MapUtils;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_DEC;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_DECIMAL;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_NUMBER;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbTypeConverter.DM_NUMERIC;

/** @deprecated instead by {@link DmdbTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class DamengDataTypeConvertor implements DataTypeConvertor<String> {
    public static final String PRECISION = "precision";
    public static final String SCALE = "scale";
    public static final Integer DEFAULT_PRECISION = 38;
    public static final Integer DEFAULT_SCALE = 18;

    @Override
    public String getIdentity() {
        return DatabaseIdentifier.DAMENG;
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String dataType) {
        return toSeaTunnelType(field, dataType, Collections.emptyMap());
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String dataType, Map<String, Object> properties) {
        Integer precision = null;
        Integer scale = null;
        switch (dataType.toUpperCase()) {
            case DM_NUMERIC:
            case DM_NUMBER:
            case DM_DECIMAL:
            case DM_DEC:
                precision = MapUtils.getInteger(properties, PRECISION, DEFAULT_PRECISION);
                scale = MapUtils.getInteger(properties, SCALE, DEFAULT_SCALE);
                break;
            default:
                break;
        }
        BasicTypeDefine typeDefine =
                BasicTypeDefine.builder()
                        .name(field)
                        .columnType(dataType)
                        .dataType(dataType)
                        .length(precision == null ? null : precision.longValue())
                        .precision(precision == null ? null : precision.longValue())
                        .scale(scale)
                        .build();

        return DmdbTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field, SeaTunnelDataType<?> dataType, Map<String, Object> properties) {
        Long precision = MapUtils.getLong(properties, PRECISION);
        Integer scale = MapUtils.getInteger(properties, SCALE);
        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(dataType)
                        .columnLength(precision)
                        .scale(scale)
                        .nullable(true)
                        .build();

        BasicTypeDefine typeDefine = DmdbTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }
}
