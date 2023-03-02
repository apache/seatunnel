/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.file.s3.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertException;
import org.apache.seatunnel.api.table.catalog.DataTypeConvertor;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.google.auto.service.AutoService;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@AutoService(DataTypeConvertor.class)
public class S3DataTypeConvertor implements DataTypeConvertor<SeaTunnelRowType> {
    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(String connectorDataType) {
        checkNotNull(connectorDataType, "connectorDataType can not be null");
        return CatalogTableUtil.parseDataType(connectorDataType);
    }

    @Override
    public SeaTunnelDataType<?> toSeaTunnelType(
            SeaTunnelRowType connectorDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        return connectorDataType;
    }

    @Override
    public SeaTunnelRowType toConnectorType(
            SeaTunnelDataType<?> seaTunnelDataType, Map<String, Object> dataTypeProperties)
            throws DataTypeConvertException {
        // transform SeaTunnelDataType to SeaTunnelRowType
        if (!(seaTunnelDataType instanceof SeaTunnelRowType)) {
            throw DataTypeConvertException.convertToConnectorDataTypeException(seaTunnelDataType);
        }
        return (SeaTunnelRowType) seaTunnelDataType;
    }

    @Override
    public String getIdentity() {
        return "S3";
    }
}
