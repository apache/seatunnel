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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.utils;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;

import org.apache.hugegraph.structure.constant.Cardinality;
import org.apache.hugegraph.structure.constant.DataType;

/**
 * Maps HugeGraph property-key types to SeaTunnel types for the source read path. Shared by schema
 * validation (declared vs. server) and schema auto-discovery (deriving the row type from the server
 * label when {@code schema} is omitted) so both use identical mapping rules.
 */
public final class HugeGraphTypeConverter {

    private HugeGraphTypeConverter() {}

    /**
     * Maps a HugeGraph property type + cardinality to a SeaTunnel type. A {@code LIST}/{@code SET}
     * cardinality becomes {@code array<element>}; {@code SINGLE} (or null) stays scalar.
     */
    public static SeaTunnelDataType<?> toSeaTunnelType(DataType dataType, Cardinality cardinality) {
        SeaTunnelDataType<?> scalar = toSeaTunnelScalarType(dataType);
        if (cardinality == null || cardinality == Cardinality.SINGLE) {
            return scalar;
        }
        // BLOB elements would produce byte[][], which downstream SeaTunnel operators do not
        // uniformly handle — reject with a clear message rather than a mysterious CCE later.
        if (dataType == DataType.BLOB) {
            throw new HugeGraphConnectorException(
                    HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                    String.format(
                            "Property type BLOB with cardinality %s is not supported for reads.",
                            cardinality));
        }
        return ArrayType.of(scalar);
    }

    public static SeaTunnelDataType<?> toSeaTunnelScalarType(DataType dataType) {
        switch (dataType) {
            case TEXT:
                return BasicType.STRING_TYPE;
            case INT:
                return BasicType.INT_TYPE;
            case LONG:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case DATE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case UUID:
                return BasicType.STRING_TYPE;
            case BLOB:
                return PrimitiveByteArrayType.INSTANCE;
            default:
                throw new HugeGraphConnectorException(
                        HugeGraphConnectorErrorCode.INVALID_GRAPH_SCHEMA,
                        String.format(
                                "Unsupported HugeGraph property type for source: %s", dataType));
        }
    }
}
