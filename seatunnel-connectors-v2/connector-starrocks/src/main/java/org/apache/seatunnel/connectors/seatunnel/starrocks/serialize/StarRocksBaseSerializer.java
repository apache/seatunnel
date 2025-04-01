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
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.utils.DateUtils;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.TimeUtils;
import org.apache.seatunnel.connectors.seatunnel.starrocks.exception.StarRocksConnectorException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

public class StarRocksBaseSerializer {
    private DateUtils.Formatter dateFormatter = DateUtils.Formatter.YYYY_MM_DD;

    private DateTimeFormatter dateTimeFormatter =
            new DateTimeFormatterBuilder()
                    .appendPattern("yyyy-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                    .toFormatter();

    private TimeUtils.Formatter timeFormatter = TimeUtils.Formatter.HH_MM_SS;

    protected Object convert(SeaTunnelDataType dataType, Object val) {
        if (val == null) {
            return null;
        }
        switch (dataType.getSqlType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
            case BOOLEAN:
            case STRING:
                return val;
            case DATE:
                return DateUtils.toString((LocalDate) val, dateFormatter);
            case TIME:
                return TimeUtils.toString((LocalTime) val, timeFormatter);
            case TIMESTAMP:
                return ((LocalDateTime) val).format(dateTimeFormatter);
            case ARRAY:
            case MAP:
                return JsonUtils.toJsonString(val);
            case BYTES:
                return new String((byte[]) val);
            default:
                throw new StarRocksConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        dataType + " is not supported ");
        }
    }
}
