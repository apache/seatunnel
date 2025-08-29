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

package org.apache.seatunnel.connectors.sensorsdata.format.exception;

import org.apache.seatunnel.common.exception.SeaTunnelErrorCode;

public enum SensorsDataErrorCode implements SeaTunnelErrorCode {
    DATA_TYPE_CAST_FIELD("SENSORS_DATA-01", "Value type does not match column type"),
    UNSUPPORTED_RECORD_TYPE("SENSORS_DATA-02", "Unsupported record type"),
    EVENT_NAME_NOT_SET("SENSORS_DATA-03", "Event name not set"),
    ILLEGAL_ARGUMENT("SENSORS_DATA-04", "Illegal argument"),
    UNKNOWN_SOURCE_FIELD("SENSORS_DATA-05", "Unknown source field"),
    MISSING_NECESSARY_FIELD("SENSORS_DATA-06", "Missing necessary field"),
    ;

    private final String code;

    private final String description;

    SensorsDataErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }
}
