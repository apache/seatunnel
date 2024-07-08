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

package org.apache.seatunnel.connectors.seatunnel.emqx.catalog;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.emqx.config.Config;

import com.google.auto.service.AutoService;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The data type convertor of Emqx, only fields defined in schema has the type. e.g.
 *
 * <pre>
 * schema = {
 *    fields {
 *      name = "string"
 *      age = "int"
 *    }
 * }
 * </pre>
 *
 * <p>Right now the data type of kafka is SeaTunnelType, so we don't need to convert the data type.
 */
@AutoService(DataTypeConvertor.class)
public class EmqxDataTypeConvertor implements DataTypeConvertor<SeaTunnelDataType<?>> {

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        return SeaTunnelDataTypeConvertorUtil.deserializeSeaTunnelDataType(
                field, connectorDataType);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            String field,
            SeaTunnelDataType<?> connectorDataType,
            Map<String, Object> dataTypeProperties) {
        return connectorDataType;
    }

    @Override
    public SeaTunnelDataType<?> toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties) {
        return seaTunnelDataType;
    }

    @Override
    public String getIdentity() {
        return Config.CONNECTOR_IDENTITY;
    }
}
