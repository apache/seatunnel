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
package org.apache.seatunnel.connectors.seatunnel.fluss.source;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussBaseOptions;

import com.alibaba.fluss.row.BinaryString;
import com.alibaba.fluss.row.Decimal;
import com.alibaba.fluss.row.TimestampLtz;
import com.alibaba.fluss.row.TimestampNtz;
import com.alibaba.fluss.types.BinaryType;
import com.alibaba.fluss.types.CharType;
import com.alibaba.fluss.types.DataType;
import com.alibaba.fluss.types.LocalZonedTimestampType;
import com.alibaba.fluss.types.TimeType;
import com.alibaba.fluss.types.TimestampType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public final class FlussTypeConverter {

    private FlussTypeConverter() {}

    public static SeaTunnelDataType<?> toSeaTunnelType(String fieldName, DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
                return BasicType.BOOLEAN_TYPE;
            case TINYINT:
                return BasicType.BYTE_TYPE;
            case SMALLINT:
                return BasicType.SHORT_TYPE;
            case INTEGER:
                return BasicType.INT_TYPE;
            case BIGINT:
                return BasicType.LONG_TYPE;
            case FLOAT:
                return BasicType.FLOAT_TYPE;
            case DOUBLE:
                return BasicType.DOUBLE_TYPE;
            case CHAR:
            case STRING:
                return BasicType.STRING_TYPE;
            case BINARY:
            case BYTES:
                return PrimitiveByteArrayType.INSTANCE;
            case DECIMAL:
                com.alibaba.fluss.types.DecimalType decimalType =
                        (com.alibaba.fluss.types.DecimalType) dataType;
                return new DecimalType(decimalType.getPrecision(), decimalType.getScale());
            case DATE:
                return LocalTimeType.LOCAL_DATE_TYPE;
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTimeType.LOCAL_TIME_TYPE;
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return LocalTimeType.LOCAL_DATE_TIME_TYPE;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return LocalTimeType.OFFSET_DATE_TIME_TYPE;
            default:
                throw CommonError.unsupportedDataType(
                        FlussBaseOptions.CONNECTOR_IDENTITY,
                        dataType.getTypeRoot().name(),
                        fieldName);
        }
    }

    public static Long columnLength(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case CHAR:
                return (long) ((CharType) dataType).getLength();
            case BINARY:
                return (long) ((BinaryType) dataType).getLength();
            case DECIMAL:
                return (long) ((com.alibaba.fluss.types.DecimalType) dataType).getPrecision();
            default:
                return null;
        }
    }

    public static Integer columnScale(DataType dataType) {
        switch (dataType.getTypeRoot()) {
            case DECIMAL:
                return ((com.alibaba.fluss.types.DecimalType) dataType).getScale();
            case TIME_WITHOUT_TIME_ZONE:
                return ((TimeType) dataType).getPrecision();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampType) dataType).getPrecision();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return ((LocalZonedTimestampType) dataType).getPrecision();
            default:
                return null;
        }
    }

    public static Object toSeaTunnelValue(String fieldName, DataType dataType, Object value) {
        if (value == null) {
            return null;
        }
        switch (dataType.getTypeRoot()) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case BINARY:
            case BYTES:
                return value;
            case CHAR:
            case STRING:
                return ((BinaryString) value).toString();
            case DECIMAL:
                return ((Decimal) value).toBigDecimal();
            case DATE:
                return LocalDate.ofEpochDay(((Integer) value).longValue());
            case TIME_WITHOUT_TIME_ZONE:
                return LocalTime.ofNanoOfDay(((Integer) value).longValue() * 1_000_000L);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return ((TimestampNtz) value).toLocalDateTime();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return OffsetDateTime.ofInstant(((TimestampLtz) value).toInstant(), ZoneOffset.UTC);
            default:
                throw CommonError.unsupportedDataType(
                        FlussBaseOptions.CONNECTOR_IDENTITY,
                        dataType.getTypeRoot().name(),
                        fieldName);
        }
    }
}
