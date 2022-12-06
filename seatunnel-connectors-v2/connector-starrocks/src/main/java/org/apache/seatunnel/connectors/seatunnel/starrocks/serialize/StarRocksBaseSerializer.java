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

package org.apache.seatunnel.connectors.seatunnel.starrocks.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.DateTimeUtils;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import lombok.Builder;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

public class StarRocksBaseSerializer {
    @Builder.Default
    private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;
    @Builder.Default
    private DateTimeUtils.Formatter dateTimeFormatter = DateTimeUtils.Formatter.YYYY_MM_DD_HH_MM_SS;
    @Builder.Default
    private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;

    protected String convert(SeaTunnelDataType dataType, Object val) {
        if (val == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
                return String.valueOf(((Number) val).shortValue());
            case INT:
                return String.valueOf(((Number) val).intValue());
            case BIGINT:
                return String.valueOf(((Number) val).longValue());
            case FLOAT:
                return String.valueOf(((Number) val).floatValue());
            case DOUBLE:
                return String.valueOf(((Number) val).doubleValue());
            case DECIMAL:
            case BOOLEAN:
                return val.toString();
            case DATE:
                return DateUtils.toString((LocalDate) val, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) val, timeFormatter);
            case TIMESTAMP:
                return DateTimeUtils.toString((LocalDateTime) val, dateTimeFormatter);
            case STRING:
                return (String) val;
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(val);
            case BYTES:
                return new String((byte[]) val);
            default:
                throw new StarRocksConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, dataType + " is not supported ");
        }
    }
}
