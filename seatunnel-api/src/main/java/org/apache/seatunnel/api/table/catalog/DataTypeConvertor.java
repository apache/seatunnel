/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import java.util.Map;

/**
 * @deprecated instead by {@link org.apache.seatunnel.api.table.converter.TypeConverter}
 * @param <T>
 */
@Deprecated
public interface DataTypeConvertor<T> {

    /**
     * Transfer the data type from connector to SeaTunnel.
     *
     * @param field The field name of the column
     * @param connectorDataType e.g. "int", "varchar(255)"
     * @return the data type of SeaTunnel
     */
    SeaTunnelDataType<?> toSeaTunnelType(String field, String connectorDataType);

    /**
     * Transfer the data type from connector to SeaTunnel.
     *
     * @param field The field name of the column
     * @param connectorDataType origin data type
     * @param dataTypeProperties origin data type properties, e.g. precision, scale, length
     * @return SeaTunnel data type
     */
    // todo: If the origin data type contains the properties, we can remove the dataTypeProperties.
    SeaTunnelDataType<?> toSeaTunnelType(
            String field, T connectorDataType, Map<String, Object> dataTypeProperties);

    /**
     * Transfer the data type from SeaTunnel to connector.
     *
     * @param field The field name of the column
     * @param seaTunnelDataType seaTunnel data type
     * @param dataTypeProperties seaTunnel data type properties, e.g. precision, scale, length
     * @return origin data type
     */
    // todo: If the SeaTunnel data type contains the properties, we can remove the
    // dataTypeProperties.
    T toConnectorType(
            String field,
            SeaTunnelDataType<?> seaTunnelDataType,
            Map<String, Object> dataTypeProperties);

    String getIdentity();
}
