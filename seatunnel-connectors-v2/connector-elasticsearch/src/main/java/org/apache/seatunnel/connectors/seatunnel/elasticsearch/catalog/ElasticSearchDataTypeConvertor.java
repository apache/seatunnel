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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;

import java.util.Map;

public class ElasticSearchDataTypeConvertor implements DataTypeConvertor<SeaTunnelDataType<?>> {

    private static final ElasticSearchDataTypeConvertor INSTANCE = new ElasticSearchDataTypeConvertor();

    private ElasticSearchDataTypeConvertor() {

    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        checkNotNull(connectorDataType);
        return SeaTunnelSchema.parseTypeByString(connectorDataType);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(SeaTunnelDataType<?> connectorDataType, Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        return connectorDataType;
    }

    @Override
    public SeaTunnelDataType<?> toConnectorType(SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties) throws DataTypeConvertException {
        return seaTunnelDataType;
    }

    public static ElasticSearchDataTypeConvertor getInstance() {
        return INSTANCE;
    }
}
