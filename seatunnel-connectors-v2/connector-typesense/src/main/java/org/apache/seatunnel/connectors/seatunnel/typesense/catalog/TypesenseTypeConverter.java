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

package org.apache.seatunnel.connectors.seatunnel.typesense.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeConverter;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.converter.TypeConverter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseType;

import com.google.auto.service.AutoService;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseType.INT32;
import static org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseType.INT64;

@AutoService(TypeConverter.class)
public class TypesenseTypeConverter implements BasicTypeConverter<BasicTypeDefine<TypesenseType>> {
    public static final TypesenseTypeConverter INSTANCE = new TypesenseTypeConverter();

    @Override
    public String identifier() {
        return "Typesense";
    }

    @Override
    public Column convert(BasicTypeDefine<TypesenseType> typeDefine) {
        PhysicalColumn.PhysicalColumnBuilder builder =
                PhysicalColumn.builder()
                        .name(typeDefine.getName())
                        .sourceType(typeDefine.getColumnType())
                        .nullable(typeDefine.isNullable())
                        .defaultValue(typeDefine.getDefaultValue())
                        .comment(typeDefine.getComment());
        String type = typeDefine.getDataType().toLowerCase();
        switch (type) {
            case INT32:
                builder.dataType(BasicType.INT_TYPE);
                break;
            case INT64:
                builder.dataType(BasicType.LONG_TYPE);
                break;
            case TypesenseType.FLOAT:
                builder.dataType(BasicType.FLOAT_TYPE);
                break;
            case TypesenseType.BOOL:
                builder.dataType(BasicType.BOOLEAN_TYPE);
                break;
            case TypesenseType.OBJET:
                Map<String, BasicTypeDefine<TypesenseType>> typeInfo =
                        (Map) typeDefine.getNativeType().getOptions();
                SeaTunnelRowType object =
                        new SeaTunnelRowType(
                                typeInfo.keySet().toArray(new String[0]),
                                typeInfo.values().stream()
                                        .map(this::convert)
                                        .map(Column::getDataType)
                                        .toArray(SeaTunnelDataType<?>[]::new));
                builder.dataType(object);
                break;
            case TypesenseType.STRING:
            case TypesenseType.IMAGE:
            default:
                builder.dataType(BasicType.STRING_TYPE);
                break;
        }
        return builder.build();
    }

    @Override
    public BasicTypeDefine<TypesenseType> reconvert(Column column) {
        throw new UnsupportedOperationException("Unsupported operation");
    }
}
