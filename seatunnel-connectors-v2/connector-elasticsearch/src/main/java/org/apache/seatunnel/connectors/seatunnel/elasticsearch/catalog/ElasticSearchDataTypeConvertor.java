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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;

import com.google.auto.service.AutoService;

import java.util.Map;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** @deprecated instead by {@link ElasticSearchTypeConverter} */
@Deprecated
@AutoService(DataTypeConvertor.class)
public class ElasticSearchDataTypeConvertor implements DataTypeConvertor<String> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        return toSeaTunnelType(field, connectorDataType, null);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field, String connectorDataType, Map<String, Object> dataTypeProperties) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        BasicTypeDefine<EsType> typeDefine =
                BasicTypeDefine.<EsType>builder()
                        .name(field)
                        .columnType(connectorDataType)
                        .dataType(connectorDataType)
                        .build();

        return ElasticSearchTypeConverter.INSTANCE.convert(typeDefine).getDataType();
    }

    @Override
    public String toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        checkNotNull(seaTunnelDataType, "seaTunnelDataType can not be null");
        Column column =
                PhysicalColumn.builder()
                        .name(field)
                        .dataType(seaTunnelDataType)
                        .nullable(true)
                        .build();
        BasicTypeDefine<EsType> typeDefine = ElasticSearchTypeConverter.INSTANCE.reconvert(column);
        return typeDefine.getColumnType();
    }

    @Override
    public String getIdentity() {
        return "Elasticsearch";
    }
}
